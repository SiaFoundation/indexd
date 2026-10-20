package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/spaces"
)

const sqlSpaces = `SELECT s.space_key, a.public_key, s.seed, s.signature, s.encrypted_name, s.encrypted_metadata, m.encrypted_key,
 COALESCE(m.permission_level, 3), s.created_at, s.updated_at
 FROM spaces s JOIN accounts a ON a.id = s.owner_account_id
 LEFT JOIN space_members m ON m.space_id = s.id AND m.account_id = $1 AND m.revoked_at IS NULL
 WHERE a.deleted_at IS NULL AND (s.owner_account_id = $1 OR m.account_id IS NOT NULL)`

// AddSpace creates a space owned by account with the given key, derivation seed,
// encrypted name, and encrypted metadata. The owner must sign the space identity.
func (s *Store) AddSpace(account proto.Account, req spaces.AddSpaceRequest) error {
	if !types.PublicKey(account).VerifyHash(req.SigHash(account), req.Signature) {
		return spaces.ErrInvalidSignature
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}
		_, err = tx.Exec(ctx, `INSERT INTO spaces (space_key, owner_account_id, seed, signature, encrypted_name, encrypted_metadata) VALUES ($1, $2, $3, $4, $5, $6)`, sqlHash256(req.Key), id, sqlHash256(req.Seed), sqlSignature(req.Signature), req.EncryptedName, req.EncryptedMetadata)
		return err
	})
}

// Spaces returns a page of spaces owned by or shared with account, newest first.
func (s *Store) Spaces(account proto.Account, offset, limit int) (result []spaces.Space, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}
		rows, err := tx.Query(ctx, sqlSpaces+` ORDER BY s.id DESC LIMIT $2 OFFSET $3`, id, limit, offset)
		if err != nil {
			return err
		}
		result, err = pgx.CollectRows(rows, scanSpace)
		return err
	})
	return
}

// Space returns a space accessible to account, including its encrypted key
// for accepted members.
func (s *Store) Space(account proto.Account, key types.Hash256) (space spaces.Space, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		_, _, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Read, true)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, sqlSpaces+` AND s.space_key = $2`, callerID, sqlHash256(key))
		if err != nil {
			return err
		}
		space, err = pgx.CollectExactlyOneRow(rows, scanSpace)
		return err
	})
	return
}

// UpdateSpace replaces a space's encrypted name and metadata. The owner and admins may update it.
func (s *Store) UpdateSpace(account proto.Account, key types.Hash256, name, metadata []byte) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaces.Admin, false)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE spaces SET encrypted_name = $2, encrypted_metadata = $3, updated_at = NOW() WHERE id = $1`, id, name, metadata)
		return err
	})
}

// InviteSpaceMember invites a registered account with the given permission and
// encrypted space key, replacing any pending invitation. Owners and admins may
// invite accounts registered with the same app ID as the owner. The creation
// time must be no more than five minutes behind server time, and expiration must
// be in the future.
func (s *Store) InviteSpaceMember(account proto.Account, key types.Hash256, recipient proto.Account, permission spaces.Permission, encryptedKey []byte, createdAt, expiresAt time.Time, signature types.Signature) error {
	if time.Since(createdAt) > 5*time.Minute {
		return spaces.ErrInvitationTimestampTooOld
	} else if !expiresAt.After(time.Now()) {
		return spaces.ErrInvitationExpired
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Admin, false)
		if err != nil {
			return err
		}
		recipientID, deleted, err := accountID(ctx, tx, recipient)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		} else if recipientID == ownerID {
			return spaces.ErrPermission
		}
		var sameApp, member bool
		if err := tx.QueryRow(ctx, `SELECT recipient.app_id = owner.app_id,
   EXISTS (SELECT 1 FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL)
   FROM accounts recipient, accounts owner WHERE recipient.id = $2 AND owner.id = $3`, id, recipientID, ownerID).Scan(&sameApp, &member); err != nil {
			return err
		} else if !sameApp || member {
			return spaces.ErrPermission
		}
		_, err = tx.Exec(ctx, `INSERT INTO space_invitations (space_id, account_id, inviter_account_id, permission_level, encrypted_key, created_at, expires_at, signature)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
   ON CONFLICT (space_id, account_id) DO UPDATE SET inviter_account_id = EXCLUDED.inviter_account_id,
   permission_level = EXCLUDED.permission_level, encrypted_key = EXCLUDED.encrypted_key,
   created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at, signature = EXCLUDED.signature, updated_at = NOW()`, id, recipientID, callerID, permission, encryptedKey, createdAt.Truncate(time.Microsecond), expiresAt.Truncate(time.Microsecond), sqlSignature(signature))
		return err
	})
}

// SpaceInvitations returns a page of unexpired pending invitations for account, newest first.
func (s *Store) SpaceInvitations(account proto.Account, offset, limit int) (invitations []spaces.Invitation, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}
		rows, err := tx.Query(ctx, `SELECT s.space_key, a.public_key, i.permission_level, i.encrypted_key, i.signature, i.expires_at, i.created_at, i.updated_at
   FROM space_invitations i JOIN spaces s ON s.id = i.space_id
   JOIN accounts a ON a.id = i.inviter_account_id JOIN accounts owner ON owner.id = s.owner_account_id
   WHERE i.account_id = $1 AND i.expires_at > clock_timestamp() AND a.deleted_at IS NULL AND owner.deleted_at IS NULL
   ORDER BY i.created_at DESC, i.space_id DESC LIMIT $2 OFFSET $3`, id, limit, offset)
		if err != nil {
			return err
		}
		invitations, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (v spaces.Invitation, err error) {
			v.Recipient = account
			err = row.Scan((*sqlHash256)(&v.SpaceKey), (*sqlPublicKey)(&v.Inviter), &v.Permission, &v.EncryptedKey, (*sqlSignature)(&v.Signature), &v.ExpiresAt, &v.CreatedAt, &v.UpdatedAt)
			return
		})
		return err
	})
	return
}

// AcceptSpaceInvitation accepts account's invitation using the space key
// re-encrypted with its app key. The invitation must be unexpired and can only be accepted once.
func (s *Store) AcceptSpaceInvitation(account proto.Account, key types.Hash256, encryptedKey []byte) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Read, false)
		if err != nil && !errors.Is(err, spaces.ErrPermission) {
			return err
		}
		res, err := tx.Exec(ctx, `WITH invitation AS (
   DELETE FROM space_invitations WHERE space_id = $1 AND account_id = $2 AND expires_at > clock_timestamp()
   AND EXISTS (SELECT 1 FROM accounts WHERE id = inviter_account_id AND deleted_at IS NULL)
   RETURNING permission_level, inviter_account_id, created_at
  ) INSERT INTO space_members (space_id, account_id, permission_level, encrypted_key, invited_by, invited_at)
  SELECT $1, $2, permission_level, $3, a.public_key, i.created_at FROM invitation i JOIN accounts a ON a.id = i.inviter_account_id
  ON CONFLICT (space_id, account_id) DO UPDATE SET permission_level = EXCLUDED.permission_level,
  encrypted_key = EXCLUDED.encrypted_key, invited_by = EXCLUDED.invited_by,
  invited_at = EXCLUDED.invited_at, revoked_at = NULL, created_at = NOW(), updated_at = NOW()`, id, callerID, encryptedKey)
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return spaces.ErrInvitationNotFound
		}
		return nil
	})
}

// DeleteSpaceInvitation cancels a pending invitation. Only the space owner
// or the recipient may cancel it.
func (s *Store) DeleteSpaceInvitation(account proto.Account, key types.Hash256, recipient proto.Account) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Read, false)
		if err != nil && !errors.Is(err, spaces.ErrPermission) {
			return err
		} else if callerID != ownerID && account != recipient {
			return spaces.ErrPermission
		}
		res, err := tx.Exec(ctx, `DELETE FROM space_invitations i USING accounts a WHERE i.space_id = $1 AND i.account_id = a.id AND a.public_key = $2`, id, sqlPublicKey(recipient))
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return spaces.ErrInvitationNotFound
		}
		return nil
	})
}

// SpaceMembers returns a page of active members. Account must have read permission.
func (s *Store) SpaceMembers(account proto.Account, key types.Hash256, offset, limit int) (members []spaces.Member, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaces.Read, true)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT a.public_key, m.invited_by, m.invited_at, m.permission_level, m.created_at, m.updated_at
   FROM space_members m JOIN accounts a ON a.id = m.account_id
   WHERE m.space_id = $1 AND m.revoked_at IS NULL AND a.deleted_at IS NULL
   ORDER BY m.account_id LIMIT $2 OFFSET $3`, id, limit, offset)
		if err != nil {
			return err
		}
		members, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (v spaces.Member, err error) {
			err = row.Scan((*sqlPublicKey)(&v.Account), (*sqlPublicKey)(&v.InvitedBy), &v.InvitedAt, &v.Permission, &v.CreatedAt, &v.UpdatedAt)
			return
		})
		return err
	})
	return
}

// UpdateSpaceMember changes an active member's permission. Only the owner may do so.
func (s *Store) UpdateSpaceMember(account proto.Account, key types.Hash256, member proto.Account, permission spaces.Permission) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaces.Owner, false)
		if err != nil {
			return err
		}
		res, err := tx.Exec(ctx, `UPDATE space_members m SET permission_level = $3, updated_at = NOW()
   FROM accounts a WHERE m.space_id = $1 AND m.account_id = a.id AND a.public_key = $2 AND m.revoked_at IS NULL`, id, sqlPublicKey(member), permission)
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return spaces.ErrMemberNotFound
		}
		return nil
	})
}

// DeleteSpaceMember revokes a member's access. The owner may revoke any member;
// admins may revoke other non-admin members. Neither may revoke themselves or the owner.
func (s *Store) DeleteSpaceMember(account proto.Account, key types.Hash256, member proto.Account) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Admin, false)
		if err != nil {
			return err
		}
		memberID, _, err := accountID(ctx, tx, member)
		if err != nil {
			return err
		} else if memberID == ownerID || memberID == callerID {
			return spaces.ErrPermission
		}
		var permission spaces.Permission
		err = tx.QueryRow(ctx, `SELECT permission_level FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL`, id, memberID).Scan(&permission)
		if errors.Is(err, pgx.ErrNoRows) {
			return spaces.ErrMemberNotFound
		} else if err != nil {
			return err
		} else if callerID != ownerID && permission == spaces.Admin {
			return spaces.ErrPermission
		}
		_, err = tx.Exec(ctx, `UPDATE space_members SET revoked_at = NOW(), updated_at = NOW() WHERE space_id = $1 AND account_id = $2`, id, memberID)
		return err
	})
}

func spaceAccess(ctx context.Context, tx *txn, account proto.Account, key types.Hash256, permission spaces.Permission, readOnly bool) (id, ownerID, callerID int64, err error) {
	var deleted bool
	callerID, deleted, err = accountID(ctx, tx, account)
	if err != nil {
		return
	} else if deleted {
		err = accounts.ErrNotFound
		return
	}
	lock := " FOR UPDATE OF s"
	if readOnly {
		lock = " FOR SHARE OF s"
	}
	err = tx.QueryRow(ctx, `SELECT s.id, s.owner_account_id FROM spaces s
 JOIN accounts a ON a.id = s.owner_account_id
 WHERE s.space_key = $1 AND a.deleted_at IS NULL`+lock, sqlHash256(key)).Scan(&id, &ownerID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = spaces.ErrNotFound
	}
	if err != nil || callerID == ownerID {
		return
	}
	var level spaces.Permission
	err = tx.QueryRow(ctx, `SELECT permission_level FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL`, id, callerID).Scan(&level)
	if errors.Is(err, pgx.ErrNoRows) {
		err = spaces.ErrPermission
	} else if err == nil && level < permission {
		err = spaces.ErrPermission
	}
	return
}

func scanSpace(row pgx.CollectableRow) (v spaces.Space, err error) {
	err = row.Scan((*sqlHash256)(&v.Key), (*sqlPublicKey)(&v.Owner), (*sqlHash256)(&v.Seed), (*sqlSignature)(&v.Signature), &v.EncryptedName, &v.EncryptedMetadata, &v.EncryptedKey, &v.Permission, &v.CreatedAt, &v.UpdatedAt)
	return
}

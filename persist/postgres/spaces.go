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

const sqlSpaces = `SELECT s.space_key, a.public_key, s.signature, s.encrypted_name, s.encrypted_metadata, m.encrypted_key,
 m.permission_level, s.created_at, s.updated_at
 FROM spaces s JOIN accounts a ON a.id = s.owner_account_id
 JOIN space_members m ON m.space_id = s.id AND m.account_id = $1 AND m.revoked_at IS NULL
 WHERE a.deleted_at IS NULL`

// AddSpace creates a space and its owner membership with the encrypted space
// key, name, and metadata. The shared fields must be signed with the space key.
func (s *Store) AddSpace(account proto.Account, req spaces.AddSpaceRequest) error {
	if !req.Key.VerifyHash(req.SigHash(account), req.Signature) {
		return spaces.ErrInvalidSignature
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}
		_, err = tx.Exec(ctx, `WITH space AS (
 INSERT INTO spaces (space_key, owner_account_id, signature, encrypted_name, encrypted_metadata)
 VALUES ($1, $2, $3, $4, $5) RETURNING id, created_at
) INSERT INTO space_members (space_id, account_id, permission_level, encrypted_key, invited_by, invited_at)
 SELECT id, $2, $6, $7, $8, created_at FROM space`, sqlPublicKey(req.Key), id, sqlSignature(req.Signature), req.EncryptedName, req.EncryptedMetadata, spaces.Owner, req.EncryptedKey, sqlPublicKey(account))
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

// Space returns a space accessible to account, including its encrypted key.
func (s *Store) Space(account proto.Account, key types.PublicKey) (space spaces.Space, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		_, _, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Read, true)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, sqlSpaces+` AND s.space_key = $2`, callerID, sqlPublicKey(key))
		if err != nil {
			return err
		}
		space, err = pgx.CollectExactlyOneRow(rows, scanSpace)
		return err
	})
	return
}

// UpdateSpace replaces a space's encrypted name, metadata, and space-key signature.
// The owner and admins may update it.
func (s *Store) UpdateSpace(account proto.Account, key types.PublicKey, name, metadata []byte, signature types.Signature) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, _, err := spaceAccess(ctx, tx, account, key, spaces.Admin, false)
		if err != nil {
			return err
		}
		space := spaces.Space{Key: key, EncryptedName: name, EncryptedMetadata: metadata}
		if err := tx.QueryRow(ctx, `SELECT public_key FROM accounts WHERE id = $1`, ownerID).Scan((*sqlPublicKey)(&space.Owner)); err != nil {
			return err
		} else if !key.VerifyHash(space.SigHash(), signature) {
			return spaces.ErrInvalidSignature
		}
		_, err = tx.Exec(ctx, `UPDATE spaces SET encrypted_name = $2, encrypted_metadata = $3, signature = $4, updated_at = NOW() WHERE id = $1`, id, name, metadata, sqlSignature(signature))
		return err
	})
}

// InviteSpaceMember invites a registered account with the given permission and
// encrypted space key, replacing any pending invitation. Owners and admins may
// invite accounts registered with the same app ID as the owner.
func (s *Store) InviteSpaceMember(invite spaces.Invitation) error {
	if invite.Permission < spaces.Read || invite.Permission > spaces.Admin {
		return spaces.ErrPermission
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, invite.Inviter, invite.SpaceKey, spaces.Admin, false)
		if err != nil {
			return err
		} else if callerID != ownerID && invite.Permission == spaces.Admin {
			return spaces.ErrPermission
		}
		recipientID, deleted, err := accountID(ctx, tx, invite.Recipient)
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
   created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at, signature = EXCLUDED.signature`, id, recipientID, callerID, invite.Permission, invite.EncryptedKey, invite.CreatedAt.Truncate(time.Microsecond), invite.ExpiresAt.Truncate(time.Microsecond), sqlSignature(invite.Signature))
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
		rows, err := tx.Query(ctx, `SELECT s.space_key, a.public_key, i.permission_level, i.encrypted_key, i.signature, i.expires_at, i.created_at
   FROM space_invitations i JOIN spaces s ON s.id = i.space_id
   JOIN accounts a ON a.id = i.inviter_account_id JOIN accounts owner ON owner.id = s.owner_account_id
   WHERE i.account_id = $1 AND i.expires_at > clock_timestamp() AND a.deleted_at IS NULL AND owner.deleted_at IS NULL
   ORDER BY i.created_at DESC, i.space_id DESC LIMIT $2 OFFSET $3`, id, limit, offset)
		if err != nil {
			return err
		}
		invitations, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (v spaces.Invitation, err error) {
			v.Recipient = account
			err = row.Scan((*sqlPublicKey)(&v.SpaceKey), (*sqlPublicKey)(&v.Inviter), &v.Permission, &v.EncryptedKey, (*sqlSignature)(&v.Signature), &v.ExpiresAt, &v.CreatedAt)
			return
		})
		return err
	})
	return
}

// AcceptSpaceInvitation accepts account's invitation using the space key
// re-encrypted with spaces.DeriveMemberKey. The invitation must be unexpired and can only be accepted once.
func (s *Store) AcceptSpaceInvitation(account proto.Account, key types.PublicKey, encryptedKey []byte) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Read, false)
		if err != nil && !errors.Is(err, spaces.ErrPermission) {
			return err
		}
		res, err := tx.Exec(ctx, `WITH invitation AS (
   DELETE FROM space_invitations WHERE space_id = $1 AND account_id = $2 AND expires_at > clock_timestamp()
   AND EXISTS (SELECT 1 FROM accounts WHERE id = inviter_account_id AND deleted_at IS NULL)
   AND (inviter_account_id = $4 OR EXISTS (
    SELECT 1 FROM space_members m WHERE m.space_id = $1 AND m.account_id = space_invitations.inviter_account_id
    AND m.permission_level = $5 AND m.revoked_at IS NULL
   ))
   RETURNING permission_level, inviter_account_id, created_at
  ) INSERT INTO space_members (space_id, account_id, permission_level, encrypted_key, invited_by, invited_at)
  SELECT $1, $2, permission_level, $3, a.public_key, i.created_at FROM invitation i JOIN accounts a ON a.id = i.inviter_account_id
  ON CONFLICT (space_id, account_id) DO UPDATE SET permission_level = EXCLUDED.permission_level,
  encrypted_key = EXCLUDED.encrypted_key, invited_by = EXCLUDED.invited_by,
  invited_at = EXCLUDED.invited_at, revoked_at = NULL, created_at = NOW(), updated_at = NOW()`, id, callerID, encryptedKey, ownerID, spaces.Admin)
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
func (s *Store) DeleteSpaceInvitation(account proto.Account, key types.PublicKey, recipient proto.Account) error {
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
func (s *Store) SpaceMembers(account proto.Account, key types.PublicKey, offset, limit int) (members []spaces.Member, err error) {
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

// UpdateSpaceMember changes an active member's permission. Admins may change
// non-admin members' permissions; only the owner may grant or change admin access.
func (s *Store) UpdateSpaceMember(account proto.Account, key types.PublicKey, member proto.Account, permission spaces.Permission) error {
	if permission < spaces.Read || permission > spaces.Admin {
		return spaces.ErrPermission
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, spaces.Admin, false)
		if err != nil {
			return err
		} else if callerID == ownerID && account == member {
			return spaces.ErrPermission
		}
		if callerID != ownerID {
			if permission == spaces.Admin {
				return spaces.ErrPermission
			}
			memberID, _, err := accountID(ctx, tx, member)
			if err != nil {
				return err
			} else if memberID == ownerID {
				return spaces.ErrPermission
			}
			var current spaces.Permission
			err = tx.QueryRow(ctx, `SELECT permission_level FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL`, id, memberID).Scan(&current)
			if errors.Is(err, pgx.ErrNoRows) {
				return spaces.ErrMemberNotFound
			} else if err != nil {
				return err
			} else if current == spaces.Admin {
				return spaces.ErrPermission
			}
		}
		res, err := tx.Exec(ctx, `UPDATE space_members m SET permission_level = $3, updated_at = NOW()
   FROM accounts a WHERE m.space_id = $1 AND m.account_id = a.id AND a.public_key = $2 AND m.revoked_at IS NULL`, id, sqlPublicKey(member), permission)
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return spaces.ErrMemberNotFound
		}
		if permission < spaces.Admin {
			_, err = tx.Exec(ctx, `DELETE FROM space_invitations i USING accounts a
 WHERE i.space_id = $1 AND i.inviter_account_id = a.id AND a.public_key = $2`, id, sqlPublicKey(member))
			return err
		}
		return nil
	})
}

// DeleteSpaceMember revokes a member's access. The owner may revoke any member;
// admins may revoke other non-admin members. Members may revoke themselves, but
// the owner cannot be revoked.
func (s *Store) DeleteSpaceMember(account proto.Account, key types.PublicKey, member proto.Account) error {
	permissionRequired := spaces.Admin
	if account == member {
		permissionRequired = spaces.Read
	}
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, permissionRequired, false)
		if err != nil {
			return err
		}
		memberID, _, err := accountID(ctx, tx, member)
		if err != nil {
			return err
		} else if memberID == ownerID {
			return spaces.ErrPermission
		}
		var permission spaces.Permission
		err = tx.QueryRow(ctx, `SELECT permission_level FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL`, id, memberID).Scan(&permission)
		if errors.Is(err, pgx.ErrNoRows) {
			return spaces.ErrMemberNotFound
		} else if err != nil {
			return err
		} else if callerID != ownerID && callerID != memberID && permission == spaces.Admin {
			return spaces.ErrPermission
		}
		if _, err := tx.Exec(ctx, `UPDATE space_members SET revoked_at = NOW(), updated_at = NOW() WHERE space_id = $1 AND account_id = $2`, id, memberID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM space_invitations WHERE space_id = $1 AND inviter_account_id = $2`, id, memberID)
		return err
	})
}

func spaceAccess(ctx context.Context, tx *txn, account proto.Account, key types.PublicKey, permission spaces.Permission, readOnly bool) (id, ownerID, callerID int64, err error) {
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
 WHERE s.space_key = $1 AND a.deleted_at IS NULL`+lock, sqlPublicKey(key)).Scan(&id, &ownerID)
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
	err = row.Scan((*sqlPublicKey)(&v.Key), (*sqlPublicKey)(&v.Owner), (*sqlSignature)(&v.Signature), &v.EncryptedName, &v.EncryptedMetadata, &v.EncryptedKey, &v.Permission, &v.CreatedAt, &v.UpdatedAt)
	return
}

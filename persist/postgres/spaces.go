package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
)

// Space permissions grant read, write, delete, and admin access cumulatively.
const (
	SpaceRead SpacePermission = iota
	SpaceWrite
	SpaceDelete
	SpaceAdmin
	spaceOwner
)

const sqlSpaces = `SELECT s.space_key, a.public_key, s.seed, s.encrypted_metadata, m.encrypted_key,
 COALESCE(m.permission_level, 3), s.created_at, s.updated_at
 FROM spaces s JOIN accounts a ON a.id = s.owner_account_id
 LEFT JOIN space_members m ON m.space_id = s.id AND m.account_id = $1 AND m.revoked_at IS NULL
 WHERE a.deleted_at IS NULL AND (s.owner_account_id = $1 OR m.account_id IS NOT NULL)`

// Errors returned by space operations.
var (
	ErrSpaceNotFound           = errors.New("space not found")
	ErrSpacePermission         = errors.New("insufficient space permission")
	ErrSpaceInvitationNotFound = errors.New("space invitation not found")
	ErrSpaceMemberNotFound     = errors.New("space member not found")
)

// SpacePermission is a member's level of access to a space.
type SpacePermission uint8

// Space describes a space and the requesting account's access to it.
type Space struct {
	Key               types.Hash256
	Owner             proto.Account
	Seed              types.Hash256
	EncryptedMetadata []byte
	EncryptedKey      []byte
	Permission        SpacePermission
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// SpaceInvitation is a pending invitation to join a space.
type SpaceInvitation struct {
	SpaceKey     types.Hash256
	Inviter      proto.Account
	Permission   SpacePermission
	EncryptedKey []byte
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// SpaceMember is an active member of a space.
type SpaceMember struct {
	Account    proto.Account
	Permission SpacePermission
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// AddSpace creates a space owned by account with the given key, derivation seed,
// and encrypted metadata.
func (s *Store) AddSpace(account proto.Account, key, seed types.Hash256, metadata []byte) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}
		_, err = tx.Exec(ctx, `INSERT INTO spaces (space_key, owner_account_id, seed, encrypted_metadata) VALUES ($1, $2, $3, $4)`, sqlHash256(key), id, sqlHash256(seed), metadata)
		return err
	})
}

// Spaces returns a page of spaces owned by or shared with account, newest first.
func (s *Store) Spaces(account proto.Account, offset, limit int) (spaces []Space, err error) {
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
		spaces, err = pgx.CollectRows(rows, scanSpace)
		return err
	})
	return
}

// Space returns a space accessible to account, including its encrypted key
// for accepted members.
func (s *Store) Space(account proto.Account, key types.Hash256) (space Space, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		_, _, callerID, err := spaceAccess(ctx, tx, account, key, SpaceRead)
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

// UpdateSpace replaces a space's encrypted metadata. The owner and admins may update it.
func (s *Store) UpdateSpace(account proto.Account, key types.Hash256, metadata []byte) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, SpaceAdmin)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE spaces SET encrypted_metadata = $2, updated_at = NOW() WHERE id = $1`, id, metadata)
		return err
	})
}

// InviteSpaceMember invites a registered account with the given permission and
// encrypted space key, replacing any pending invitation. Owners and admins may
// invite accounts registered with the same app ID as the owner.
func (s *Store) InviteSpaceMember(account proto.Account, key types.Hash256, recipient proto.Account, permission SpacePermission, encryptedKey []byte) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, SpaceAdmin)
		if err != nil {
			return err
		}
		recipientID, deleted, err := accountID(ctx, tx, recipient)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		} else if recipientID == ownerID {
			return ErrSpacePermission
		}
		var sameApp, member bool
		if err := tx.QueryRow(ctx, `SELECT recipient.app_id = owner.app_id,
   EXISTS (SELECT 1 FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL)
   FROM accounts recipient, accounts owner WHERE recipient.id = $2 AND owner.id = $3`, id, recipientID, ownerID).Scan(&sameApp, &member); err != nil {
			return err
		} else if !sameApp || member {
			return ErrSpacePermission
		}
		_, err = tx.Exec(ctx, `INSERT INTO space_invitations (space_id, account_id, inviter_account_id, permission_level, encrypted_key)
   VALUES ($1, $2, $3, $4, $5)
   ON CONFLICT (space_id, account_id) DO UPDATE SET inviter_account_id = EXCLUDED.inviter_account_id,
   permission_level = EXCLUDED.permission_level, encrypted_key = EXCLUDED.encrypted_key, updated_at = NOW()`, id, recipientID, callerID, permission, encryptedKey)
		return err
	})
}

// SpaceInvitations returns a page of pending invitations for account, newest first.
func (s *Store) SpaceInvitations(account proto.Account, offset, limit int) (invitations []SpaceInvitation, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, deleted, err := accountID(ctx, tx, account)
		if err != nil {
			return err
		} else if deleted {
			return accounts.ErrNotFound
		}
		rows, err := tx.Query(ctx, `SELECT s.space_key, a.public_key, i.permission_level, i.encrypted_key, i.created_at, i.updated_at
   FROM space_invitations i JOIN spaces s ON s.id = i.space_id
   JOIN accounts a ON a.id = i.inviter_account_id JOIN accounts owner ON owner.id = s.owner_account_id
   WHERE i.account_id = $1 AND a.deleted_at IS NULL AND owner.deleted_at IS NULL
   ORDER BY i.created_at DESC, i.space_id DESC LIMIT $2 OFFSET $3`, id, limit, offset)
		if err != nil {
			return err
		}
		invitations, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (v SpaceInvitation, err error) {
			err = row.Scan((*sqlHash256)(&v.SpaceKey), (*sqlPublicKey)(&v.Inviter), &v.Permission, &v.EncryptedKey, &v.CreatedAt, &v.UpdatedAt)
			return
		})
		return err
	})
	return
}

// AcceptSpaceInvitation accepts account's invitation using the space key
// re-encrypted with its app key. The invitation can only be accepted once.
func (s *Store) AcceptSpaceInvitation(account proto.Account, key types.Hash256, encryptedKey []byte) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, callerID, err := spaceAccess(ctx, tx, account, key, SpaceRead)
		if err != nil && !errors.Is(err, ErrSpacePermission) {
			return err
		}
		res, err := tx.Exec(ctx, `WITH invitation AS (
   DELETE FROM space_invitations WHERE space_id = $1 AND account_id = $2 RETURNING permission_level
  ) INSERT INTO space_members (space_id, account_id, permission_level, encrypted_key)
  SELECT $1, $2, permission_level, $3 FROM invitation
  ON CONFLICT (space_id, account_id) DO UPDATE SET permission_level = EXCLUDED.permission_level,
  encrypted_key = EXCLUDED.encrypted_key, revoked_at = NULL, created_at = NOW(), updated_at = NOW()`, id, callerID, encryptedKey)
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return ErrSpaceInvitationNotFound
		}
		return nil
	})
}

// DeleteSpaceInvitation cancels a pending invitation. Only the space owner
// or the recipient may cancel it.
func (s *Store) DeleteSpaceInvitation(account proto.Account, key types.Hash256, recipient proto.Account) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, SpaceRead)
		if err != nil && !errors.Is(err, ErrSpacePermission) {
			return err
		} else if callerID != ownerID && account != recipient {
			return ErrSpacePermission
		}
		res, err := tx.Exec(ctx, `DELETE FROM space_invitations i USING accounts a WHERE i.space_id = $1 AND i.account_id = a.id AND a.public_key = $2`, id, sqlPublicKey(recipient))
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return ErrSpaceInvitationNotFound
		}
		return nil
	})
}

// SpaceMembers returns a page of active members. Account must have read permission.
func (s *Store) SpaceMembers(account proto.Account, key types.Hash256, offset, limit int) (members []SpaceMember, err error) {
	err = s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, SpaceRead)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT a.public_key, m.permission_level, m.created_at, m.updated_at
   FROM space_members m JOIN accounts a ON a.id = m.account_id
   WHERE m.space_id = $1 AND m.revoked_at IS NULL AND a.deleted_at IS NULL
   ORDER BY m.account_id LIMIT $2 OFFSET $3`, id, limit, offset)
		if err != nil {
			return err
		}
		members, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (v SpaceMember, err error) {
			err = row.Scan((*sqlPublicKey)(&v.Account), &v.Permission, &v.CreatedAt, &v.UpdatedAt)
			return
		})
		return err
	})
	return
}

// UpdateSpaceMember changes an active member's permission. Only the owner may do so.
func (s *Store) UpdateSpaceMember(account proto.Account, key types.Hash256, member proto.Account, permission SpacePermission) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, _, _, err := spaceAccess(ctx, tx, account, key, spaceOwner)
		if err != nil {
			return err
		}
		res, err := tx.Exec(ctx, `UPDATE space_members m SET permission_level = $3, updated_at = NOW()
   FROM accounts a WHERE m.space_id = $1 AND m.account_id = a.id AND a.public_key = $2 AND m.revoked_at IS NULL`, id, sqlPublicKey(member), permission)
		if err != nil {
			return err
		} else if res.RowsAffected() == 0 {
			return ErrSpaceMemberNotFound
		}
		return nil
	})
}

// DeleteSpaceMember revokes a member's access. The owner may revoke any member;
// admins may revoke other non-admin members. Neither may revoke themselves or the owner.
func (s *Store) DeleteSpaceMember(account proto.Account, key types.Hash256, member proto.Account) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		id, ownerID, callerID, err := spaceAccess(ctx, tx, account, key, SpaceAdmin)
		if err != nil {
			return err
		}
		memberID, _, err := accountID(ctx, tx, member)
		if err != nil {
			return err
		} else if memberID == ownerID || memberID == callerID {
			return ErrSpacePermission
		}
		var permission SpacePermission
		err = tx.QueryRow(ctx, `SELECT permission_level FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL`, id, memberID).Scan(&permission)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSpaceMemberNotFound
		} else if err != nil {
			return err
		} else if callerID != ownerID && permission == SpaceAdmin {
			return ErrSpacePermission
		}
		_, err = tx.Exec(ctx, `UPDATE space_members SET revoked_at = NOW(), updated_at = NOW() WHERE space_id = $1 AND account_id = $2`, id, memberID)
		return err
	})
}

func spaceAccess(ctx context.Context, tx *txn, account proto.Account, key types.Hash256, permission SpacePermission) (id, ownerID, callerID int64, err error) {
	var deleted bool
	callerID, deleted, err = accountID(ctx, tx, account)
	if err != nil {
		return
	} else if deleted {
		err = accounts.ErrNotFound
		return
	}
	err = tx.QueryRow(ctx, `SELECT s.id, s.owner_account_id FROM spaces s
 JOIN accounts a ON a.id = s.owner_account_id
 WHERE s.space_key = $1 AND a.deleted_at IS NULL FOR UPDATE OF s`, sqlHash256(key)).Scan(&id, &ownerID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrSpaceNotFound
	}
	if err != nil || callerID == ownerID {
		return
	}
	var level SpacePermission
	err = tx.QueryRow(ctx, `SELECT permission_level FROM space_members WHERE space_id = $1 AND account_id = $2 AND revoked_at IS NULL`, id, callerID).Scan(&level)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrSpacePermission
	} else if err == nil && level < permission {
		err = ErrSpacePermission
	}
	return
}

func scanSpace(row pgx.CollectableRow) (v Space, err error) {
	err = row.Scan((*sqlHash256)(&v.Key), (*sqlPublicKey)(&v.Owner), (*sqlHash256)(&v.Seed), &v.EncryptedMetadata, &v.EncryptedKey, &v.Permission, &v.CreatedAt, &v.UpdatedAt)
	return
}

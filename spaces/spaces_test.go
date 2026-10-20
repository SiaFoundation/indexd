package spaces

import (
	"testing"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestInvitationSigHash(t *testing.T) {
	key := types.GeneratePrivateKey()
	invitation := Invitation{SpaceKey: frand.Entropy256(), Inviter: [32]byte(key.PublicKey()), Recipient: frand.Entropy256(), Permission: Write, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	signature := key.SignHash(invitation.SigHash())
	for _, change := range []func(*Invitation){
		func(i *Invitation) { i.SpaceKey[0] ^= 1 },
		func(i *Invitation) { i.Permission = Admin },
		func(i *Invitation) { i.Inviter[0] ^= 1 },
		func(i *Invitation) { i.Recipient[0] ^= 1 },
		func(i *Invitation) { i.CreatedAt = i.CreatedAt.Add(time.Microsecond) },
		func(i *Invitation) { i.ExpiresAt = i.ExpiresAt.Add(time.Microsecond) },
	} {
		modified := invitation
		change(&modified)
		if key.PublicKey().VerifyHash(modified.SigHash(), signature) {
			t.Fatal("signature accepted a modified invitation")
		}
	}
	invitation.CreatedAt = invitation.CreatedAt.UTC().Truncate(time.Microsecond)
	invitation.ExpiresAt = invitation.ExpiresAt.UTC().Truncate(time.Microsecond)
	if !key.PublicKey().VerifyHash(invitation.SigHash(), signature) {
		t.Fatal("signature rejected timestamp normalization")
	}
}

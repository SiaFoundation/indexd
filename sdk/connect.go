package sdk

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/app"
)

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		return fmt.Errorf("unsupported platform %q", runtime.GOOS)
	}
}

// A ConnectAppRequest is a request to connect an application
// to the indexer.
type ConnectAppRequest struct {
	app.RegisterAppResponse

	client *app.Client
}

// WaitForApproval waits for the user to approve the app connection request in a
// browser window that it attempts to open automatically. It will block until
// the request is either approved or denied.
// NOTE: The app should display the response URL to the user before calling this
// function in case the browser fails to open automatically.
func (cr *ConnectAppRequest) WaitForApproval(ctx context.Context) (bool, error) {
	if time.Until(cr.Expiration) <= 0 {
		return false, fmt.Errorf("request expired")
	}

	ctx, cancel := context.WithDeadline(ctx, cr.Expiration)
	defer cancel()

	openBrowser(cr.ResponseURL)
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Second):
			if ok, err := cr.client.CheckRequestStatus(ctx, cr.StatusURL); err != nil {
				return false, fmt.Errorf("failed to check request status: %w", err)
			} else if ok {
				return true, nil
			}
		}
	}
}

// Connect requests permission to connect an application to the indexer.
// If the app is already connected, it returns ((), true, nil).
func Connect(ctx context.Context, indexerURL string, appKey types.PrivateKey, meta app.RegisterAppRequest) (ConnectAppRequest, bool, error) {
	client, err := app.NewClient(indexerURL, appKey)
	if err != nil {
		return ConnectAppRequest{}, false, err
	}

	if ok, err := client.CheckAppAuth(ctx); err != nil {
		return ConnectAppRequest{}, false, fmt.Errorf("failed to check app auth: %w", err)
	} else if ok {
		return ConnectAppRequest{}, true, nil
	}

	resp, err := client.RequestAppConnection(ctx, meta)
	return ConnectAppRequest{RegisterAppResponse: resp, client: client}, false, err
}

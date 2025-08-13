# The official Go SDK for the Sia Indexd API

This SDK provides a convenient way to interact with the Indexd API, allowing
users to connect an application to the indexer and uploading data to as well as
downloading data from the Sia network.

The README provides a short overview to quickly get started using the SDK. For more information, please refer to the [Godocs](https://pkg.go.dev/go.sia.tech/indexd/sdk).

## Connecting to the Indexer

Before you can connect to the Indexer, you need to obtain an app password from
the Indexer UI. Afterwards, you can use the following code to connect your
application to the Indexer:

```go
resp, connected, err := sdk.Connect(ctx, "http://localhost:9982", sk, app.RegisterAppRequest{
  Name:        "MyApp",                     // The name of your application
  Description: "My first Sia application",  // A short description of your application
  LogoURL:     "https://my.app/logo.png",   // A URL to your application's logo to display in the UI
  ServiceURL:  "https://my.app/home",       // A URL to your application's homepage
})
if err != nil {
  log.Fatal("failed to connect app")
} else if connected {
  log.Info("app is already connected")
}

// If the app is not connected, the user needs to approve the connection by
// following the URL provided in the response.
fmt.Println("please approve app connection at the following url:", resp.ResponseURL)
connected, err := resp.WaitForApproval(ctx);
if err != nil {
	log.Fatal("failed to wait for app approval")
} else if !connected {
	log.Fatal("user denied app connection")
}
```

This code will attempat to open a browser window for the user to put in their
app password and approve the request. If the browser cannot be opened, the user
will need to manually open the URL.

## Uploading and Downloading Data

Once an application is connected, it can upload and download to and from the
network. The following is a minimal example on how to upload and download a
file using the default settings.

```go
// create the client
client, err := sdk.NewSDK("http://localhost:9982", sk)
if err != nil {
	log.Fatal("failed to create SDK client", zap.Error(err))
}

// open a file to upload
srcFile, err := os.Open("path/to/src.dat")
if err != nil {
	log.Fatal("failed to open the file")
}
defer srcFile.Close()

// upload the file
metadata, err := client.Upload(context.Background(), file)
if err != nil {
	log.Fatal("failed to upload file")
}

// open a file to download to
dstFile, err := os.Open("path/to/dst.dat")
if err != nil {
	log.Fatal("failed to open the file")
}
defer dstFile.Close()

// download the file
err = client.Download(context.Background(), dstFile, metadata)
if err != nil {
	log.Fatal("failed to download file")
}
```

The `metadata` returned from the `Upload` function contains information about
the uploaded file required to download it later. Applications need to to store
this information in a database or some other persistent storage.

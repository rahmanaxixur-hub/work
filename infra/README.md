# GCS Emulator with Podman Compose

This setup runs a Google Cloud Storage emulator using Podman Compose.

## Usage

To start the emulator, run:

```
podman-compose -f podman-compose.yml up -d
```

The emulator will be available at http://localhost:4443.

## Stopping the Emulator

To stop the emulator, run:

```
podman-compose -f podman-compose.yml down
```

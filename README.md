# Consumer Go package

This wraps the message consumer logic into a small package.
Currently only contains a google pubsub backend but it can be extended for
other backends in the future

## Build

```
go build
```
Yes. That simple

## Test

With a pubsub emulator running, then...
Start a pubsub emulator with
```
gcloud beta emulators pubsub start
```

Then initialize the environment and run the tests with
```
eval $(gcloud beta emulators pubsub env-init)
go test
```
Once again, yes... that simple :)

## Documentation

```
godoc github.com/replaygaming/consumer
```
BOOM :boom:

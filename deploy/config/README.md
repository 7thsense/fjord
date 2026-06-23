# Fjord Runtime Profiles

Non-secret deployment and benchmark defaults live in this directory.

## Garage Scale Lane

`garage-scale.env` is the checked-in profile for `deploy/garage-scale.sh`.
It owns the Garage scale lane's record count, partition count, producer policy,
flush policy, and evidence location defaults.

Secrets and machine-local endpoints stay out of this directory. Provide them
through `deploy/chaos/garage.env` or the caller environment:

```sh
FJORD_PG_URL=postgresql://...
FJORD_GARAGE_SECRET=...
FJORD_GARAGE_ENDPOINT=http://...
FJORD_GARAGE_BUCKET=fjord
FJORD_GARAGE_KEY_ID=...
```

To run a different non-secret profile, pass:

```sh
FJORD_GARAGE_SCALE_CONFIG=/path/to/profile.env deploy/garage-scale.sh
```

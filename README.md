Plug thing into computer then:

```
ip link set can0 type can bitrate 125000
ifconfig can0 up
```

System dependencies are:
* python3
* pip
* virtualenv (optional)
* the can driver thing (not sure the name or how to check, hopefully it's just in your kernel already)

Create and activate virtualenv:

```
virtualenv --no-site-packages env
source env/bin/activate
```

Install the libraries:

```
pip install -r requirements.txt
```

Create a config file (e.g. `config.ini`) based on `defaults.ini` with your desired configuration.

Run it:

```
python run.py config.ini 
```

Format code (`pip install yapf` first):

```
yapf --style chromium -i run.py
```

## TODO

- [ ] tolerance if can interface is missing
- [ ] tolerance if influxdb is not available
- [ ] gracefully close the node when it restarts/exits
- [ ] don't request node info all the time, cache it and then add it into incoming node status updates, maybe periodically get it fresh again
- [ ] report health information if influxdb is unavailable
- [ ] make sure the restarting works when run in a systemd environment

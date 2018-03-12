Plug thing into computer then:

```
ip link set can0 type can bitrate 125000
ifconfig can0 up
```

Run it:

```
python hello.py foo.ini 
```

Format code:

```
yapf --style chromium -i hello.py
```

# cauli-worker

The Rust worker for [cauli](https://pypi.org/project/cauli/), a Python task
queue. One binary embeds CPython and runs async, sync and cpu tasks from Redis.

Install it with `pip install cauli`, which pulls in the matching
`cauli-worker` wheel. Documentation:
https://github.com/bilalwork31-cyber/Cauli

```bash
cauli-worker -A myproj.tasks:app -c 50
```

Licensed under either of Apache-2.0 or MIT at your option.

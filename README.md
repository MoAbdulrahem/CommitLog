# CommitLog
An implementation of [ARIES](https://cs.stanford.edu/people/chrismre/cs345/rl/aries.pdf) Write Ahead logging. A fault-tolerance algorithm that guranttes durability and atomicity of transactions.

This project is done as a module for [DibiBase](https://github.com/DibiBase/dibibase), A distributed Wide-column NoSQL database engine.

# Setup and usage
After cloning this repository, go to the build folder and build the project
```bash
cd build
```

```bash
make
```

```bash
./Commitlog-Prototype
```

# Event Pipeline

Details still to be added

### Installing

Clone the repository with 
```
git clone git@github.com:marcus-vgr/EventPipeline.git
cd EventPipeline
```

When using the package for the first time you should install it:
```
python -m venv .venv
pip install -e .
```

Otherwise you just have to source it:
```
source .venv/bin/activate
```

### Generating toy data

A simple script is available to generate toy data. All data is created in chunks, which means you can choose as many events as you want without having to worry about memory issues. 
```
python data/generate_data.py <nEvents> <data/filename.parquet>
```
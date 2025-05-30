<p align="center">
    <img alt="Electricity Maps" src="https://raw.githubusercontent.com/electricitymaps/electricitymaps-contrib/master/web/public/images/electricitymaps-icon.svg" width="100" />
</p>
<h1 align="center">
  Electricity Maps Zone Finder
</h1>

<p align="center">
This tool allows you to find the Electricity Maps zone for a set of coordinates without having to call the API.
</p>

---
## Acknowledgement
This code is modified from https://github.com/electricitymaps/zone-finder to run on python

---

## Introduction

This small script replicates the coordinate look up functionality of the [Electricity Maps API](https://docs.electricitymaps.com) to allow clients to run the lookup without having to call the API.

## How to use

1. Clone or [download](https://github.com/electricitymaps/zone-finder/archive/refs/heads/main.zip) this repository
2. Add your coordinates to the `data.csv` file (see example in file)
3. Run `python3 index.py` on the directory containing index.py
4. The coresponding Electricity Maps zone will be written to `data.csv` for each row

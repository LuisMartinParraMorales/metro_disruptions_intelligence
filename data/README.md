# data/

This folder is intentionally left almost empty. **We do NOT store the 30 GB of raw GTFS JSON files here.**

## Where the real data lives

By default, our code will look for a top‐level key `data_dir` in `config/default.yaml` and `config/local.yaml`. 
If you have put `local.yaml` in `config/` (gitignored), it might look like:

config/local.yaml
└─ data_dir: "C:\Users\Luis.ParraMorales\OneDrive - Imperial College London\Dissertation-LDNLT5CG2147KJJ\Data"

That `Data` folder (on OneDrive) is expected to have these subfolders:
RAIL_RT_ALERTS/
└─ RAIL_RT_ALERTS/ ← (the concatenated folder with all alerts JSON)

RAIL_RT_TRIP_UPDATES/
└─ RAIL_RT_TRIP_UPDATES/

RAIL_RT_VEHICLE_POSITIONS/
└─ RAIL_RT_VEHICLE_POSITIONS/

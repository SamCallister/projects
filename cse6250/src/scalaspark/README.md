# scala source for project

### Getting Started

This source can run inside the class docker image.

There are currently a few modes for running the executable:
1. sbt "run inspect" will list a couple values from each of the standard datasets, plus the first antibiotic/culture.
2. sbt "run saveVitals" will extract vital stats and save to a directory of csv files (must be combined).
3. sbt "run inspectVitals" will list a couple values from the saved vitals csv (combined into one file).

### Prerequisites

1. Download the MIMIC III csv files.
2. Decompress the ones needed for the application (check src/main/scala/edu/gatech/cse6250/data/loaders.scala)
3. Move these into ./data subdirectory under build directory (= the directory containing build.sbt);
Alternatively, create a symlink to location of csv files named "data".

### Installing

Nothing to install.

## Deployment

TBD


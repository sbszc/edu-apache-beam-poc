#  Equifax Latam POC

Transformations of Scenario 1 and 2 described on POC-22 are now implemented.

To run Main projects with arguments please modify Arguments param with something like this:

> --scenario=TWO --sourceFile="src/main/resources/test.csv" --targetFile="src/main/resources/" --years="2013,2014"

Notes:
- Consider on which date time format is being retrieve from the .csv it seems to be yyyy-MM-dd HH:mm:ss.SSS 
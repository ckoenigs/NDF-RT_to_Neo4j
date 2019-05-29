# NDF-RT_to_Neo4j
This script extracts all information from the XML ndf-rt source and generates CSV files. Also, a cypher file is generated which allows integrating the data with the Neo4j-shell or with the cypher-shell.

The data were downloaded from https://evs.nci.nih.gov/ftp1/NDF-RT/. 

The resulted graph looks like this:


![er_diagram](https://github.com/ckoenigs/NDF-RT_to_Neo4j/blob/master/ndf-rt.png)

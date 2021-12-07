# Logs transformation using PySparks


A project with examples of using few commonly used data manipulation/processing/transformation in PySpark.<br>


Use generate/generate_data.py to generate data<br>

Logs generating into folder <b>raw_logs</b><br>

And they looks like:<br>

- Version line (v1)<br>
- Metadata line {"batchWatermarkMs":0,"batchTimestampMs":1626616200136,...}<br>
- Commit line “topicName” : {“partitionNumber” : “readOffset”}<br>

Topics presented in file <b>topics</b>

import json


with open('jobs_results.json') as jr:
   results = json.loads(jr.read())
print ','.join(results.keys())


import json, sys
uuidDict = json.loads(sys.stdin.read())
print(uuidDict['uuid'])

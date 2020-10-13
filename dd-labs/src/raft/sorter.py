
import os
ROOT = "Logs/"
for file in os.listdir(ROOT):
	with open(ROOT+file, 'r') as f:
		lns = f.readlines()
		cnt = 0
		for ln in lns:
			if 'PASS' in ln or 'Passed' in ln:
				cnt += 1
		if cnt == 3:
			continue
		with open("sortedLogs/"+file, 'w+') as nf:
			if lns[0][0] != '1':
				lns[0] = '0|' + lns[0]
			for i in range(1, len(lns)):
				if lns[i][0] != '1':
					lns[i] = lns[i-1].split('|')[0] +'|'+ lns[i]
			lns.sort(key=lambda x: int(x.split('|')[0]))
			nf.writelines(lns)
			



for i in range(31):
	with open("Logs/log" + str(i) + ".txt", 'r') as f:
		with open("sortedLog/log" + str(i) + ".txt", 'w+') as nf:
			lns = f.readlines()
			cnt = 0
			for ln in lns:
				if 'PASS' in ln or 'Passed' in ln:
					cnt += 1
			if cnt == 3:
				continue
			if lns[0][0] != '1':
				lns[0] = '0|' + lns[0]
			for i in range(1, len(lns)):
				if lns[i][0] != '1':
					lns[i] = lns[i-1].split('|')[0] +'|'+ lns[i]
			lns.sort(key=lambda x: int(x.split('|')[0]))
			nf.writelines(lns)
			

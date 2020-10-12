rm Logs/* && rm sortedLogs/* && for i in {0..30}; do go test -run 2A > "Logs/log$i.txt"; done && python3 sorter.py

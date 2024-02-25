import csv

def makeDictFromCSV(csvFilePath):
    rows = []
    with open(csvFilePath, 'r', newline='') as csvFile:
        csvReader = csv.DictReader(csvFile)
        for row in csvReader:
            rows.append(row)
    return(rows)

if __name__ == "__main__":
    print(makeDictFromCSV("output.csv"))
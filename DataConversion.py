import csv

class csvOperations():
    def makeDictFromCSV(csvFilePath):
        rows = []
        with open(csvFilePath, 'r', newline='') as csvFile:
            csvReader = csv.DictReader(csvFile)
            for index, row in enumerate(csvReader):
                if index > 0:
                    rows.append(row)
        return(rows)

if __name__ == "__main__":
    print(csvOperations.makeDictFromCSV("output.csv"))
import csv
import numpy as np
from anonymizer import Anonymizer

FILE_PATH = "./smart_hospital_dataset.csv"
NEW_FILE_PATH = "./anonymized_smart_hospital_dataset.csv"

myFileAsArray = []
with open(FILE_PATH, 'r', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        myFileAsArray.append(row)

numpy_array = np.array(myFileAsArray, dtype=str)

anonymizer = Anonymizer()



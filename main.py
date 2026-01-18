import csv
import numpy as np
import pandas as pd
from anonymizer import Anonymizer

FILE_PATH = "./smart_hospital_dataset.csv"
NEW_FILE_PATH = "./anonymized_smart_hospital_dataset.csv"

df = pd.read_csv(FILE_PATH)

anonymizer = Anonymizer()

df = anonymizer.pseudonymization(df, "patient_identifier")
df = anonymizer.anonymize_sensor_locations(df, "sensor_location")

# En retirant le lit on obtient un k=31
# ça semble suffisant, car si on garde que le
# ward on se retrouve avec un k = 954 beaucoup
# trop généralisé

k = anonymizer.verify_k_anonymization(df, "sensor_location")
print(f"K value: {k}")

# La l-diversité s'occupe de cacher ce qu'on peut apprendre sur un groupe !
# ici par exemple si tous nos patients
# le but est de préserver la privacy
# quelles sont les données sensibles ici ?
# on va supposer que ce sont les données des capteurs
# Ici on doit regarder pour chaque groupe si il y a des groupes qui présentent
# les mêmes données sensibles

# on peut choisir un l entre 3 et 5 courant et on vérifie !
l = 5
is_l_verification_valid = anonymizer.verify_l_anonymization(
    df, "sensor_location", ["blood_pressure", "heart_rate"], l
)

print(f"L-diversity (L={l}) : {is_l_verification_valid}")

# avec cette approche on a bien la bonne vérification mais notre algorithme ne fait
# le rapprochement entre un patient qui a 71 et 12 de bool_pressure, il faudrait qu'on
# un des clés moyen : .., haut : .. pour rendre plus résistant


df.to_csv(NEW_FILE_PATH)

# chiffrer les données, on peut utiliser une librairie ou pas ?
# doit-on implémenter l'algo de chiffrement ?

# sauvegarder dans la db ? Quelle db ?

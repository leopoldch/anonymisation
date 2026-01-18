import hashlib
from pandas import DataFrame


class Anonymizer:

    def pseudonymization(self, df: DataFrame, key: str) -> DataFrame:
        df[key] = df[key].map(
            lambda pseudo: hashlib.sha256(str(pseudo).encode()).hexdigest()
        )
        return df

    def anonymize_sensor_locations(self, df: DataFrame, key: str) -> DataFrame:
        sensor_locations = df[key]
        # toutes les chambres sont composées de Ward-X, Room-Y, Bed-Z
        # dans un premier temps on généralise en enlevant l'information des lits
        # et on regarde combien de fois nos données sont réutilisées en moyenne

        def remove_bed_from_location(location: str):
            location_array = location.split(",")
            return f"{location_array[0]},{location_array[1]}"

        sensor_locations = sensor_locations.map(remove_bed_from_location)

        df[key] = sensor_locations
        return df

    def verify_k_anonymization(self, df: DataFrame, keys: list) -> int:
        group_sizes = df.groupby(keys).size()
        k_value = group_sizes.min()
        return k_value

    def verify_l_anonymization(
        self, df: DataFrame, group_key: str, sensible_data_keys: list, l: int = 3
    ) -> bool:
        groups = df.groupby(group_key)

        for _, group_df in groups:
            # pour tous les groupes on vérifie qu'il y ait au moins
            # l données sensibles différentes

            different_sensible_data_groups = group_df.groupby(sensible_data_keys).size()

            if len(different_sensible_data_groups) < l:
                return False

        return True

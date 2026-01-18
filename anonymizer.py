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

        def remove_bed_from_location(location: str) -> str:
            location_array = location.split(",")
            return f"{location_array[0]},{location_array[1]}"

        sensor_locations = sensor_locations.map(remove_bed_from_location)

        df[key] = sensor_locations
        return df

    def verify_k_anonymization(self, df: DataFrame, keys: list) -> int:
        group_sizes = df.groupby(keys).size()
        k_value = group_sizes.min()
        return k_value

    def _prepare_grouping_blood_pressure(self, df: DataFrame, key: str) -> DataFrame:
        df_copy = df.copy()

        blood_pressure = df_copy[key]

        def associate_category(blood_pressure: str) -> str:
            top, bottom = blood_pressure.split("/")
            top = int(top)
            bottom = int(bottom)
            return get_blood_pressure_category(top, bottom)

        blood_pressure = blood_pressure.map(associate_category)
        df_copy[key] = blood_pressure

        return df_copy

    def _prepare_grouping_heart_rate(self, df: DataFrame, key: str) -> DataFrame:
        df_copy = df.copy()

        heart_rate = df_copy[key]

        def associate_category(heart_rate: str) -> str:
            heart_rate = heart_rate.split(" ")[0]  # removes ' bpm' from string
            heart_rate = int(heart_rate)
            return get_heart_rate_category(heart_rate)

        heart_rate = heart_rate.map(associate_category)
        df_copy[key] = heart_rate

        return df_copy

    def prepare_sensible_data(
        self, df: DataFrame, blood_pressure_key: str, heart_rate_key: str
    ) -> DataFrame:
        df_copy = self._prepare_grouping_blood_pressure(df, blood_pressure_key)
        df_copy = self._prepare_grouping_heart_rate(df_copy, heart_rate_key)

        return df_copy

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


def get_heart_rate_category(heart_rate: int) -> str:
    LOW = 60
    NORMAL = 100

    if heart_rate < LOW:
        return "LOW"

    if heart_rate < NORMAL:
        return "NORMAL"

    return "HIGH"


def get_blood_pressure_category(top: int, bottom: int) -> str:
    SEUIL_VERY_HIGH_TOP, SEUIL_VERY_HIGH_BOTTOM = 160, 100
    SEUIL_HIGH_TOP, SEUIL_HIGH_BOTTOM = 140, 90
    SEUIL_NORMAL_TOP, SEUIL_NORMAL_BOTTOM = 120, 80

    if top >= SEUIL_VERY_HIGH_TOP or bottom >= SEUIL_VERY_HIGH_BOTTOM:
        return "VERY HIGH"

    if top >= SEUIL_HIGH_TOP or bottom >= SEUIL_HIGH_BOTTOM:
        return "HIGH"

    if top >= SEUIL_NORMAL_TOP or bottom >= SEUIL_NORMAL_BOTTOM:
        return "HIGH NORMAL"

    return "NORMAL"

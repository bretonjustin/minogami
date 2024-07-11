import csv
import os
import time

import requests
from datetime import datetime, timedelta
import pytz
import gspread
from gspread import Worksheet, Cell
from gspread_formatting import *

INDEX_STATION_CEHQ = 7
INDEX_STATION_VIGILANCE = 9

INDEX_THRESHOLD_MIN = 4
INDEX_THRESHOLD_MAX = 5

INDEX_DEBIT_ACTUEL_CEHQ = 12
INDEX_DEBIT_ACTUEL_VIGILANCE = 13

INDEX_DEBIT_24H_CEHQ = 14
INDEX_DEBIT_24H_VIGILANCE = 15

INDEX_DEBIT_48H_CEHQ = 16
INDEX_DEBIT_48H_VIGILANCE = 17

INDEX_DEBIT_72H_CEHQ = 18
INDEX_DEBIT_72H_VIGILANCE = 19

CEHQ_BASE_LINK = "https://www.cehq.gouv.qc.ca/depot/suivihydro/bd/JSON/"
VIGILANCE_BASE_LINK = "https://inedit-ro.geo.msp.gouv.qc.ca/station_details_readings_api?id=eq."

csv_header = []


def read_rivers() -> list:
    rivers = []
    with open('rivers.csv', 'r', encoding="ISO-8859-1") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            rivers.append(row)

    global csv_header
    csv_header = rivers[0]

    # remove header
    rivers.pop(0)

    return list(rivers)


def fetch_json_from_url(url: str) -> dict:
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None


def get_datetime(delta_h=0):
    utc_now = datetime.now(pytz.utc)
    future_utc_time = utc_now + timedelta(hours=delta_h)
    montreal = pytz.timezone('America/Montreal')
    future_montreal_time = future_utc_time.astimezone(montreal)
    return future_montreal_time


def fetch_cehq(station: int) -> list:
    try:
        station = str(station).zfill(6)
        url = f"{CEHQ_BASE_LINK}{station}.json"

        data = fetch_json_from_url(url)

        if data is not None:
            diffusion = data['diffusion']
            prevision = data['prevision']

            diffusion = sorted(diffusion, key=lambda x: (x['dateDonnee'], x['heureDonnee']), reverse=True)

            prevision = sorted(prevision, key=lambda x: x['datePrevision'], reverse=True)

            debit_actuel = diffusion[0]['donnee']

            prevision_24h_date = get_datetime(24).strftime('%Y-%m-%d 09:00:00')
            prevision_48h_date = get_datetime(48).strftime('%Y-%m-%d 09:00:00')
            prevision_72h_date = get_datetime(72).strftime('%Y-%m-%d 09:00:00')

            prevision_24h = next((item for item in prevision if item['datePrevision'] == prevision_24h_date), None)
            prevision_24h = prevision_24h['qMCS'] if prevision_24h is not None else 0
            prevision_48h = next((item for item in prevision if item['datePrevision'] == prevision_48h_date), None)
            prevision_48h = prevision_48h['qMCS'] if prevision_48h is not None else 0
            prevision_72h = next((item for item in prevision if item['datePrevision'] == prevision_72h_date), None)
            prevision_72h = prevision_72h['qMCS'] if prevision_72h is not None else 0

            return [debit_actuel, prevision_24h, prevision_48h, prevision_72h]

    except Exception as e:
        print("Error in cehq: " + str(station) + " " + str(e))
        return [0, 0, 0, 0]


def fetch_vigilance(station: int) -> list:
    try:
        url = f"{VIGILANCE_BASE_LINK}{station}"

        data = fetch_json_from_url(url)

        if data is not None:
            data = data[0]

            debit_actuel_list = data['valeurs_deb']
            debit_actuel_list = sorted(debit_actuel_list, key=lambda x: x['date_prise_valeur'], reverse=True)
            debit_actuel = debit_actuel_list[0]['valeur']

            debit_prevision_list = data['valeurs_deb_prev']
            # convert each datetime to America/Montreal timezone
            for debit_prevision in debit_prevision_list:
                debit_prevision['date_prise_valeur'] = datetime.strptime(debit_prevision['date_prise_valeur'], '%Y-%m-%dT%H:%M:%S')
                debit_prevision['date_prise_valeur'] = debit_prevision['date_prise_valeur'].replace(tzinfo=pytz.utc)
                debit_prevision['date_prise_valeur'] = debit_prevision['date_prise_valeur'].astimezone(pytz.timezone('America/Montreal'))
                debit_prevision['date_prise_valeur'] = debit_prevision['date_prise_valeur'].strftime('%Y-%m-%d %H:%M:%S')

            debit_prevision_list = sorted(debit_prevision_list, key=lambda x: x['date_prise_valeur'])

            prevision_24h = next((item for item in debit_prevision_list if item['date_prise_valeur'] == get_datetime(24).strftime('%Y-%m-%d 07:00:00')), None)
            prevision_24h = prevision_24h['valeur'] if prevision_24h is not None else 0
            prevision_48h = next((item for item in debit_prevision_list if item['date_prise_valeur'] == get_datetime(48).strftime('%Y-%m-%d 07:00:00')), None)
            prevision_48h = prevision_48h['valeur'] if prevision_48h is not None else 0
            prevision_72h = next((item for item in debit_prevision_list if item['date_prise_valeur'] == get_datetime(72).strftime('%Y-%m-%d 07:00:00')), None)
            prevision_72h = prevision_72h['valeur'] if prevision_72h is not None else 0

            return [debit_actuel, prevision_24h, prevision_48h, prevision_72h]

    except Exception as e:
        print("Error in vigilance: " + str(station) + " " + str(e))
        return [0, 0, 0, 0]


def fetch_river(rivers: list) -> list:
    global csv_header

    for river in rivers:
        station_cehq = river[INDEX_STATION_CEHQ]
        station_vigilance = river[INDEX_STATION_VIGILANCE]

        previsions_cehq = fetch_cehq(station_cehq)
        previsions_vigilance = fetch_vigilance(station_vigilance)

        if previsions_cehq is not None and previsions_vigilance is not None:
            river.append(str(previsions_cehq[0]))
            river.append(str(previsions_vigilance[0]))

            river.append(str(previsions_cehq[1]))
            river.append(str(previsions_vigilance[1]))

            river.append(str(previsions_cehq[2]))
            river.append(str(previsions_vigilance[2]))

            river.append(str(previsions_cehq[3]))
            river.append(str(previsions_vigilance[3]))

        time.sleep(0.5)

    csv_header.append("CEHQ Debit Actuel")
    csv_header.append("Vigilance Debit Actuel")

    csv_header.append("CEHQ Debit 24h")
    csv_header.append("Vigilance Debit 24h")

    csv_header.append("CEHQ Debit 48h")
    csv_header.append("Vigilance Debit 48h")

    csv_header.append("CEHQ Debit 72h")
    csv_header.append("Vigilance Debit 72h")
    rivers.insert(0, csv_header)
    return rivers


def export_rivers(rivers: list):
    try:
        credentials = {
            "type": "service_account",
            "project_id": str(os.environ['PROJECT_ID']),
            "private_key_id": str(os.environ['PRIVATE_KEY_ID']),
            "private_key": str(os.environ['PRIVATE_KEY']).replace('\\n', '\n'),
            "client_email": str(os.environ['CLIENT_EMAIL']),
            "client_id": str(os.environ['CLIENT_ID']),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": str(os.environ['CLIENT_X509_CERT_URL']),
            "universe_domain": "googleapis.com"
        }

        gc = gspread.service_account_from_dict(credentials)
        # gc = gspread.service_account(filename='credentials.json')
        sh = gc.create(get_datetime(0).strftime('%Y-%m-%d'), folder_id=str(os.environ['FOLDER_ID']))

        worksheet = sh.sheet1
        worksheet.update(rivers)

        format_cell_color(worksheet)
    except Exception as e:
        print("Error in export_rivers: " + str(e))
        pass


def set_cell_color(cells: list[Cell], row: int, col: int, color: tuple):
    for cell in cells:
        if cell.row == row and cell.col == col:
            cell.color = color


def convert_list_of_dict_to_list_of_list(list_of_dict: list[dict]) -> list[list]:

def format_cell_color(worksheet: Worksheet):
    cells_dict = worksheet.get_all_records()
    cells_values = list(cells_dict.values())
    cells_keys = list(cells_dict)

    cell_format_list = []

    fmt = CellFormat(
        backgroundColor=Color(1, 0.0, 0.0)
    )

    for index_row, row in enumerate(cells_values):
        if row[INDEX_DEBIT_ACTUEL_CEHQ] <= row[INDEX_THRESHOLD_MIN]:
            cell_format_list.append((cells_keys[index_row][INDEX_DEBIT_ACTUEL_CEHQ], fmt))
        if row[INDEX_DEBIT_ACTUEL_CEHQ] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_ACTUEL_CEHQ, red_color_rgb)
        if row[INDEX_DEBIT_ACTUEL_VIGILANCE] <= row[INDEX_THRESHOLD_MIN]:
            set_cell_color(cells, row_index, INDEX_DEBIT_ACTUEL_VIGILANCE, red_color_rgb)
        if row[INDEX_DEBIT_ACTUEL_VIGILANCE] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_ACTUEL_VIGILANCE, red_color_rgb)

        if row[INDEX_DEBIT_24H_CEHQ] <= row[INDEX_THRESHOLD_MIN]:
            set_cell_color(cells, row_index, INDEX_DEBIT_24H_CEHQ, red_color_rgb)
        if row[INDEX_DEBIT_24H_CEHQ] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_24H_CEHQ, red_color_rgb)
        if row[INDEX_DEBIT_24H_VIGILANCE] <= row[INDEX_THRESHOLD_MIN]:
            set_cell_color(cells, row_index, INDEX_DEBIT_24H_VIGILANCE, red_color_rgb)
        if row[INDEX_DEBIT_24H_VIGILANCE] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_24H_VIGILANCE, red_color_rgb)

        if row[INDEX_DEBIT_48H_CEHQ] <= row[INDEX_THRESHOLD_MIN]:
            set_cell_color(cells, row_index, INDEX_DEBIT_48H_CEHQ, red_color_rgb)
        if row[INDEX_DEBIT_48H_CEHQ] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_48H_CEHQ, red_color_rgb)
        if row[INDEX_DEBIT_48H_VIGILANCE] <= row[INDEX_THRESHOLD_MIN]:
            set_cell_color(cells, row_index, INDEX_DEBIT_48H_VIGILANCE, red_color_rgb)
        if row[INDEX_DEBIT_48H_VIGILANCE] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_48H_VIGILANCE, red_color_rgb)

        if row[INDEX_DEBIT_72H_CEHQ] <= row[INDEX_THRESHOLD_MIN]:
            set_cell_color(cells, row_index, INDEX_DEBIT_72H_CEHQ, red_color_rgb)
        if row[INDEX_DEBIT_72H_CEHQ] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_72H_CEHQ, red_color_rgb)
        if row[INDEX_DEBIT_72H_VIGILANCE] <= row[INDEX_THRESHOLD_MIN]:
            set_cell_color(cells, row_index, INDEX_DEBIT_72H_VIGILANCE, red_color_rgb)
        if row[INDEX_DEBIT_72H_VIGILANCE] >= row[INDEX_THRESHOLD_MAX]:
            set_cell_color(cells, row_index, INDEX_DEBIT_72H_VIGILANCE, red_color_rgb)

    worksheet.format(cells)


def main():
    rivers = read_rivers()
    rivers = fetch_river(rivers)
    export_rivers(rivers)
    print(rivers)


if __name__ == "__main__":
    main()

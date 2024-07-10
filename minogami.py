import csv
import os
import time

import requests
from datetime import datetime, timedelta
import pytz
import gspread

INDEX_STATION_CEHQ = 7
INDEX_STATION_VIGILANCE = 9

CEHQ_BASE_LINK = "https://www.cehq.gouv.qc.ca/depot/suivihydro/bd/JSON/"
VIGILANCE_BASE_LINK = "https://inedit-ro.geo.msp.gouv.qc.ca/station_details_readings_api?id=eq."

credentials = {
    "type": "service_account",
    "project_id": str(os.environ['PROJECT_ID']),
    "private_key_id": str(os.environ['PRIVATE_KEY_ID']),
    "private_key": str(os.environ['PRIVATE_KEY']),
    "client_email": str(os.environ['CLIENT_EMAIL']),
    "client_id": str(os.environ['CLIENT_ID']),
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": str(os.environ['CLIENT_X509_CERT_URL']),
    "universe_domain": "googleapis.com"
}

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
            prevision_24h = prevision_24h['qMCS']
            prevision_48h = next((item for item in prevision if item['datePrevision'] == prevision_48h_date), None)
            prevision_48h = prevision_48h['qMCS']
            prevision_72h = next((item for item in prevision if item['datePrevision'] == prevision_72h_date), None)
            prevision_72h = prevision_72h['qMCS']

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

            prevision_24h = next((item for item in debit_prevision_list if item['date_prise_valeur'] == get_datetime(24).strftime('%Y-%m-%d 10:00:00')), None)
            prevision_24h = prevision_24h['valeur']
            prevision_48h = next((item for item in debit_prevision_list if item['date_prise_valeur'] == get_datetime(48).strftime('%Y-%m-%d 10:00:00')), None)
            prevision_48h = prevision_48h['valeur']
            prevision_72h = next((item for item in debit_prevision_list if item['date_prise_valeur'] == get_datetime(72).strftime('%Y-%m-%d 10:00:00')), None)
            prevision_72h = prevision_72h['valeur']

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

            csv_header.append("CEHQ Debit Actuel")
            csv_header.append("Vigilance Debit Actuel")

            csv_header.append("CEHQ Debit 24h")
            csv_header.append("Vigilance Debit 24h")

            csv_header.append("CEHQ Debit 48h")
            csv_header.append("Vigilance Debit 48h")

            csv_header.append("CEHQ Debit 72h")
            csv_header.append("Vigilance Debit 72h")

        time.sleep(0.5)

    rivers.insert(0, csv_header)
    return rivers


def export_rivers(rivers: list):
    try:
        gc = gspread.service_account_from_dict(credentials)
        # gc = gspread.service_account(filename='credentials.json')
        sh = gc.create(get_datetime(0).strftime('%Y-%m-%d'), folder_id=str(os.environ['FOLDER_ID']))

        worksheet = sh.sheet1
        worksheet.update(rivers)
        pass
    except Exception as e:
        print("Error in export_rivers: " + str(e))
        pass


def main():
    rivers = read_rivers()
    rivers = fetch_river(rivers)
    export_rivers(rivers)
    print(rivers)


if __name__ == "__main__":
    main()

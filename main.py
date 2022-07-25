import requests,json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json',scope)
client = gspread.authorize(credentials)
sheet = client.open('eBlast Stats Monitor (HubSpot)').worksheet('Automated eBlast Monitor')

def gsheet_record():
    Google_sheet_value = sheet.get_all_values()
    store_record = Google_sheet_value[1:]
    return store_record


def update_gsheet(payloads,sheet_range):
    update_sheet = sheet.update(sheet_range,[payloads])
    return update_sheet


def get_hubspot_data(limit,offset):
    url = f"https://api.hubapi.com/marketing-emails/v1/emails/with-statistics/?hapikey=5d0af513-0ddf-41fa-b756-69b809c0125d&limit={limit}&offset={offset}"
    response = requests.request("GET", url)
    get_response_value=json.loads(response.text)
    return get_response_value

def main():
    g_data = gsheet_record()
    open = ''
    sent = ''
    open_rate = ''
    Clickthrough_Rate =  ''
    Click_Rate =  ''
    count = 2
    limit = 300
    offset = 0
    total_record = get_hubspot_data(limit,offset).get('totalCount')
    store = round(float(total_record) / 300) * 300
    if store >= 300:
        while(True):
            h_data  = get_hubspot_data(limit,offset)['objects']
            for get_googsheet_data in g_data:
                for hubspot_data in h_data:
                    if get_googsheet_data[0] in hubspot_data.get('name'):
                        if hubspot_data.get('stats'):
                            if hubspot_data.get("stats").get("counters"):
                                if hubspot_data.get("stats").get("counters").get('open'):
                                    open = hubspot_data.get("stats").get("counters").get('open')
                                else:
                                    open = 0
                                if hubspot_data.get("stats").get("counters").get('sent'):
                                    sent = hubspot_data.get("stats").get("counters").get('sent')
                                else:
                                    sent=0
                                if hubspot_data.get("stats").get("counters").get('click'):
                                    click = hubspot_data.get("stats").get("counters").get('click')
                                else:
                                    click =0

                            if hubspot_data.get("stats").get("ratios"):
                                if hubspot_data.get("stats").get("ratios").get('openratio'):
                                    open_rate = hubspot_data.get("stats").get("ratios").get('openratio')
                                else:
                                    open_rate=0
                                if hubspot_data.get("stats").get("ratios").get('clickthroughratio'):
                                    Clickthrough_Rate =  hubspot_data.get("stats").get("ratios").get('clickthroughratio')  
                                else:
                                    Clickthrough_Rate=0
                                if hubspot_data.get("stats").get("ratios").get('clickratio'):
                                    Click_Rate =  hubspot_data.get("stats").get("ratios").get('clickratio')
                                else:
                                    Click_Rate=0
            
                            payloads =[sent,open,open_rate,click,Clickthrough_Rate,Click_Rate]
                            st_range = f"J{count}:O{count}"
                            print(hubspot_data.get("id")," :: ",get_googsheet_data[0]," :: ",st_range," :: ",payloads)
                            update_gsheet(payloads,st_range)
                count+=1

            if offset == store:
                if offset > total_record:
                    for get_googsheet_data in g_data:
                        for hubspot_data in get_hubspot_data(limit,total_record-1)['objects']:
                            if get_googsheet_data[0] in hubspot_data.get("name").split(" ")[-1]:
                                if hubspot_data.get('stats'):
                                    if hubspot_data.get("stats").get("counters"):
                                        if hubspot_data.get("stats").get("counters").get('open'):
                                            open = hubspot_data.get("stats").get("counters").get('open')
                                        else:
                                            open = 0
                                        if hubspot_data.get("stats").get("counters").get('sent'):
                                            sent = hubspot_data.get("stats").get("counters").get('sent')
                                        else:
                                            sent=0
                                        if hubspot_data.get("stats").get("counters").get('click'):
                                            click = hubspot_data.get("stats").get("counters").get('click')
                                        else:
                                            click =0

                                    if hubspot_data.get("stats").get("ratios"):
                                        if hubspot_data.get("stats").get("ratios").get('openratio'):
                                            open_rate = hubspot_data.get("stats").get("ratios").get('openratio')
                                        else:
                                            open_rate=0
                                        if hubspot_data.get("stats").get("ratios").get('clickthroughratio'):
                                            Clickthrough_Rate =  hubspot_data.get("stats").get("ratios").get('clickthroughratio')  
                                        else:
                                            Clickthrough_Rate=0
                                        if hubspot_data.get("stats").get("ratios").get('clickratio'):
                                            Click_Rate =  hubspot_data.get("stats").get("ratios").get('clickratio')
                                        else:
                                            Click_Rate=0

                                    payloads =[sent,open,open_rate,click,Clickthrough_Rate,Click_Rate]
                                    st_range = f"J{count}:O{count}"
                                    print(hubspot_data.get("id")," :: ",get_googsheet_data[0]," :: ",st_range," :: ",payloads)
                                    update_gsheet(payloads,st_range)
                
                break
            offset+=300                          
    return True 
main()
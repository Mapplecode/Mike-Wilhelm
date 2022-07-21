import requests,json

sheet_id = "1kAHqaITcGjBtf3Q63zVy7echQYzXDMIZu88iiihxYy0"
def get_token():
    url = "https://accounts.google.com/o/oauth2/token?refresh_token=1//04YYq2cIu2W3KCgYIARAAGAQSNwF-L9Ir462CP71LzcAN5V83eUvL0X6sB7QSUKh3wx7s7SOJ6kTeMKyBZUn-AJiKML-RmMz6itk&redirect_uri=https://developers.google.com/oauthplayground&client_id=319825298674-b4jv0vm504ic4fpt7ub9oc1nms7qpofn.apps.googleusercontent.com&client_secret=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&grant_type=refresh_token"
    headers = {
    'Cookie': 'NID=511=Csaq4q6a5RQZoqG-fjKdR1dq9_A-5hLLb61LS9GKOFMJrfs_7hyf5bUviDd8K2AsBF6vhZM65jDRYTNKNI9ZV7F9G35kZgfkcHh9JMO7AY6gsaX1VdtOgKLs9qv3ilTmEW7ouLQI-AwGrTBWMl_SY2bnxPxEIv_RgcNZ82q2K1E; __Host-GAPS=1:BSgSEG-H0z6z7SwFbrjS2sFmzhHbJA:GWcoiIRwPrRR7-XN'
    }
    # url = "https://accounts.google.com/o/oauth2/token?refresh_token=1//04dUtOTJvhfZrCgYIARAAGAQSNwF-L9IrnbcJB3o0gYiveJqJqtGtQqT_VYIKAYSGOMgPBDPjr9IJElQLaasbEGrBzInqvydAB1k&redirect_uri=https://developers.google.com/oauthplayground&client_id=319825298674-b4jv0vm504ic4fpt7ub9oc1nms7qpofn.apps.googleusercontent.com&client_secret=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&grant_type=refresh_token"
    response = requests.request("POST", url,headers=headers)
    store =json.loads(response.text)
    access_token = store.get('access_token')
    return access_token
   
def get_data(get_tokens):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Automated eBlast Monitor!A1:$O"
    payload={}
    headers = {
    'Authorization': f'Bearer {get_tokens}'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    store =json.loads(response.text)['values']
    return store

def update_gsheet(get_tokens,empty_list,sheet_range):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values:batchUpdate"
    payload = json.dumps({
    "data": [
        {
        "values": [
            empty_list
        ],
        "range": sheet_range
        }
    ],
    "valueInputOption": "USER_ENTERED"
    })
    headers = {
    'Authorization': f'Bearer {get_tokens}',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.status_code


def get_hubspot_data(limit,offset):
    url = f"https://api.hubapi.com/marketing-emails/v1/emails/with-statistics/?hapikey=5d0af513-0ddf-41fa-b756-69b809c0125d&limit={limit}&offset={offset}"
    response = requests.request("GET", url)
    get_response_value=json.loads(response.text)
    return get_response_value

   
def main():
  
    token = get_token()
    g_data = get_data(token)[1:]
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
            # print(limit, "Offset:",offset)
            h_data  = get_hubspot_data(limit,offset)['objects']
            for j in g_data:
                for i in h_data:
                    if j[0] in i.get("name"):
                        if i.get("stats"):
                            if i.get("stats").get("counters"):
                                if i.get("stats").get("counters").get('open'):
                                    open = i.get("stats").get("counters").get('open')
                                else:
                                    open = 0
                                if i.get("stats").get("counters").get('sent'):
                                    sent = i.get("stats").get("counters").get('sent')
                                else:
                                    sent=0
                                if i.get("stats").get("counters").get('click'):
                                    click = i.get("stats").get("counters").get('click')
                                else:
                                    click =0
                            if i.get("stats").get("ratios"):
                                if i.get("stats").get("ratios").get('openratio'):
                                    open_rate = i.get("stats").get("ratios").get('openratio')
                                else:
                                    open_rate=0
                                if i.get("stats").get("ratios").get('clickthroughratio'):
                                    Clickthrough_Rate =  i.get("stats").get("ratios").get('clickthroughratio')  
                                else:
                                    Clickthrough_Rate=0
                                if i.get("stats").get("ratios").get('clickratio'):
                                    Click_Rate =  i.get("stats").get("ratios").get('clickratio')
                                else:
                                    Click_Rate=0
                            payloads =[sent,open,open_rate,click,Clickthrough_Rate,Click_Rate]
                            st_range = f"Automated eBlast Monitor!J{count}:O{count}"
                            print(i.get("id")," :: ",j[0]," :: ",st_range," :: ",payloads)
                            update_gsheet(token,payloads,st_range)
                count += 1

            if offset == store:
                if offset > total_record:
                    # print(limit,total_record)
                    for j in g_data:
                            for i in get_hubspot_data(limit,total_record-1)['objects']:
                                if j[0] in i.get("name").split(" ")[-1]:
                                    if  i.get("stats"):
                                       if i.get("stats").get("counters"):
                                        if i.get("stats").get("counters").get('open'):
                                            open = i.get("stats").get("counters").get('open')
                                        else:
                                            open = 0
                                        if i.get("stats").get("counters").get('sent'):
                                            sent = i.get("stats").get("counters").get('sent')
                                        else:
                                            sent=0
                                        if i.get("stats").get("counters").get('click'):
                                            click = i.get("stats").get("counters").get('click')
                                        else:
                                            click =0
                                    if i.get("stats").get("ratios"):
                                        if i.get("stats").get("ratios").get('openratio'):
                                            open_rate = i.get("stats").get("ratios").get('openratio')
                                        else:
                                            open_rate=0
                                        if i.get("stats").get("ratios").get('clickthroughratio'):
                                            Clickthrough_Rate =  i.get("stats").get("ratios").get('clickthroughratio')  
                                        else:
                                            Clickthrough_Rate=0
                                        if i.get("stats").get("ratios").get('clickratio'):
                                            Click_Rate =  i.get("stats").get("ratios").get('clickratio')
                                        else:
                                            Click_Rate=0
                                        payloads =[sent,open,open_rate,click,Clickthrough_Rate,Click_Rate]
                                        st_range = f"Automated eBlast Monitor!J{count}:O{count}"
                                        print(i.get("id")," :: ",j[0]," :: ",st_range," :: ",payloads)
                                        update_gsheet(token,payloads,st_range)
                break
            offset+=300
        
    return True 
main()



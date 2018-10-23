# -*- coding: utf-8 -*-
"""
Created on Tue Oct  9 11:39:30 2018

@author: TS591AQ
"""
 
import requests
import base64 
import json
import time
from math import ceil 
class UserPasswordException(Exception):
    pass

class EloquaInterface:
    def __init__(self, site_name, user_name,password):
        """Construtor para a interface do eloqua

            Parameters
            ----------
            site_name : str
               endereço do site do usuário
            user_name : str
               usuário do Eloqua
            password : str
                senha para o eloqua

        """
        self.site_name = site_name
        self.user_name = user_name
        self.password = password

    def req(self,url,method = 'get',data ={}):
        """Método para fazer uma requisição http

            Parameters
            ----------
            url : str
               endereço do site a se fazer a requisição
            method : str
               O método http a ser utilizado(o padrão é 'get')
            data : dict
                dicionário de dados contendo os parâmetros que necessários para um método post. O padrão é um dicionário vazio
             Returns
            -------
            dict
                Dicionário com a resposta do servidor 
        """
        site_name = self.site_name
        user_name = self.user_name 
        password = self.password
        # header para autenticação com senha codificada
        header = site_name + '\\' + user_name + ":"+password
        encoded_header = bytes("Basic ",'utf-8')+base64.standard_b64encode(bytes(header,  'utf-8'))
        header = {r'Authorization':encoded_header,r'Content-Type':"application/json"}
        
        if method == 'get':
            r = requests.get(url, headers=header)
        else: 
            r = requests.post(url, headers=header,data = json.dumps(data))
        response = json.loads(r.text)
        return response
    
    def get_bulk_url(self):
        """Método para adquirir o url para a api bulk

            Parameters
            ----------
            Returns
            -------
            str
                string contendo o endereço url da api bulk
        """
        
        url = "https://login.eloqua.com/id"
        root_response = self.req(url)
        return root_response['urls']['apis']['rest']['bulk'].replace('{version}','2.0')
   
    def get_standard_url(self):
        """Método para adquirir o url para a api padrão 2.0

            Parameters
            ----------
            Returns
            -------
            str
                string contendo o endereço url da api padrão 2.0
        """
        
        url = "https://login.eloqua.com/id"
        root_response = self.req(url)
        return root_response['urls']['apis']['rest']['standard'].replace('{version}','2.0')
    
    def get_campaigns(self):
        """Método para adquirir todas as campanhas do eloqua. Usamos a versão 2.0 da api

            Parameters
            ----------
            Returns
            -------
            str
                string contendo o endereço url da api padrão 2.0
        """ 
        std_url = self.get_standard_url()
        page = 1
        page_size = 500
        campaigns = []
        campaign_url = std_url + "assets/campaigns?count={}&page={}".format(page_size,page)
        root_response = self.req(campaign_url)
        total = root_response["total"]
        print("Total: {}".format(total))
        campaigns.extend(root_response["elements"])
        pages = int(ceil(total/page_size))
        if pages > 1:
            for i in range(2,pages+1):
                page = i
                campaign_url = std_url + "assets/campaigns?count={}&page={}".format(page_size,page)
                root_response = self.req(campaign_url)
                campaigns.extend(root_response["elements"])
        return campaigns
        
    def check_data(self,url,data_uri):
        """Método para verificar o status da api para exportação de dados

            Parameters
            ----------
            url : str
                url da bulk api para este usuário
            data_uri : str
                uri do dado a ser exportado
                
            Returns
            -------
            dict
                dicionário de resposta da requisição 
        """
        get_data_url = url+data_uri
        return self.req(get_data_url,method='get')   
    
    def get_data(self,url,data_uri):
        """Método para adquirir os dados necessários

            Parameters
            ----------
            url : str
                url da bulk api para este usuário
            data_uri : str
                uri do dado a ser exportado
                
            Returns
            -------
            list
                lista com todos os dados adquiridos 
        """
        #buscaremos os daos de 50 mil linhas por vez, se houver mais de que isso, estrá no próximo offset
        offset = 0
        get_data_url = url+data_uri+"/data?limit=50000&offset={}".format(offset)
        # assumimos que o a api de exportação de dados não está pronta 
        status = "pending"
        count =0
        # Enquanto o status da api estiver pending, estaremos em loop. Há um contador limite que não deixa esperarmos para sempre.  
        while status == "pending":
            check_response = self.check_data(url,data_uri)
            status = check_response["status"]
            count+=1
            time.sleep(10)
            #verifica se o timer estorou
            if(count == 60):
                status = "success"
        get_data_response = self.req(get_data_url,method='get')
        print("Resposta: count: {}, hasMore: {}".format(get_data_response["count"],get_data_response["hasMore"]))
            
        if get_data_response["totalResults"] > 0:
            data = get_data_response["items"]
            # Se houver mais linhas pendentes, ira fazer mais requisições para adquirir todas. Cada loop adquire 50k linhas.
            while get_data_response["hasMore"]:  
                #mudando o offset para buscar as proximas linhas 
                offset += 50000
                get_data_url = url+data_uri+"/data?limit=50000&offset={}".format(offset)
                get_data_response = self.req(get_data_url,method='get')
                print("Resposta: count: {}, hasMore{}".format(get_data_response["count"],get_data_response["hasMore"]))
                data.extend(get_data_response["items"])
        else:
            data = []
        return data
    
    def syc_data(self,url,export_url):
        """Método para sincronizar os dados após a criação da api de dados expecíficas

            Parameters
            ----------
            url : str
                url da bulk api para este usuário
            export_url : str
                uri do dado a ser exportado
                
            Returns
            -------
            dict
                dicionário contento a resposta da requisição 
        """
        syc_url = url + "syncs"
        data = {
                "syncedInstanceUri" : export_url
                }
        response = self.req(syc_url,data = data,method= 'post')
        return response


    def build_export(self,bulk_api_url,data):
        """Método para construir a api de dados a ser exportada

            Parameters
            ----------
            bulk_api_url : str
                url da bulk api para este usuário
            data : dict
                dicionário contento os dados necessários para a cosntrução da api             
            Returns
            -------
            dict
                dicionário contento a resposta da requisição 
        """
        bulk_end_point = "activities/exports"
        return self.req(bulk_api_url+bulk_end_point,method = 'post',data = data)

    def build_click(self,bulk_api_url) :
        """Método para construir a api de dados de clique 

            Parameters
            ----------
            bulk_api_url : str
                url da bulk api para este usuário           
            Returns
            -------
            dict
                dicionário contento a resposta da requisição 
        """
        data = {
        "name": "Bulk Activity Export - Email Clickthrough",
        "fields": {
            "ActivityId": "{{Activity.Id}}",
            "ActivityType": "{{Activity.Type}}",
            "ActivityDate": "{{Activity.CreatedAt}}",
            "EmailAddress": "{{Activity.Field(EmailAddress)}}",
            "ContactId": "{{Activity.Contact.Id}}",
            "IpAddress": "{{Activity.Field(IpAddress)}}",
            "VisitorId": "{{Activity.Visitor.Id}}",
            "VisitorExternalId": "{{Activity.Visitor.ExternalId}}",
            "EmailRecipientId": "{{Activity.Field(EmailRecipientId)}}",
            "AssetType": "{{Activity.Asset.Type}}",
            "AssetName": "{{Activity.Asset.Name}}",
            "AssetId": "{{Activity.Asset.Id}}",
            "SubjectLine": "{{Activity.Field(SubjectLine)}}",
            "EmailWebLink": "{{Activity.Field(EmailWebLink)}}",
            "EmailClickedThruLink": "{{Activity.Field(EmailClickedThruLink)}}",
            "CampaignId": "{{Activity.Campaign.Id}}",
            "ExternalId": "{{Activity.ExternalId}}",
            "EmailSendType": "{{Activity.Field(EmailSendType)}}"
            },
        "filter": "'{{Activity.Type}}' = 'EmailClickthrough'",
        }
        return self.build_export(bulk_api_url,data)  

    def build_bounce(self,bulk_api_url):
        """Método para construir a api de dados de bounce 

            Parameters
            ----------
            bulk_api_url : str
                url da bulk api para este usuário           
            Returns
            -------
            dict
                dicionário contento a resposta da requisição 
        """
        data = {
        "name": "Bulk Activity Export - Email Bounce",
        "fields": {
              "ActivityId": "{{Activity.Id}}",
              "ActivityType": "{{Activity.Type}}",
              "ActivityDate": "{{Activity.CreatedAt}}",
              "EmailAddress": "{{Activity.Field(EmailAddress)}}",
              "AssetType": "{{Activity.Asset.Type}}",
              "AssetName": "{{Activity.Asset.Name}}",
              "AssetId": "{{Activity.Asset.Id}}",
              "CampaignId": "{{Activity.Campaign.Id}}",
              "ExternalId": "{{Activity.ExternalId}}",
              "EmailRecipientId": "{{Activity.Field(EmailRecipientId)}}",
              "DeploymentId": "{{Activity.Field(EmailDeploymentId)}}",
              "SmtpErrorCode": "{{Activity.Field(SmtpErrorCode)}}",
              "SmtpStatusCode": "{{Activity.Field(SmtpStatusCode)}}",
              "SmtpMessage": "{{Activity.Field(SmtpMessage)}}"
            },
        "filter": "'{{Activity.Type}}' = 'Bounceback'",
        }
        return self.build_export(bulk_api_url,data)

    def build_sent(self,bulk_api_url,campaign_id):
        """Método para construir a api de dados de email enviados 

            Parameters
            ----------
            bulk_api_url : str
                url da bulk api para este usuário           
            Returns
            -------
            dict
                dicionário contento a resposta da requisição 
        """
        data = {
        "name": "Bulk Activity Export - Email Open",
                "fields": {
                      "ActivityId": "{{Activity.Id}}",
                      "ActivityType": "{{Activity.Type}}",
                      "ActivityDate": "{{Activity.CreatedAt}}",
                      "EmailAddress": "{{Activity.Field(EmailAddress)}}",
                      "CampaignId": "{{Activity.Campaign.Id}}",
                      "EmailSendType": "{{Activity.Field(EmailSendType)}}"
                          },
        "filter": "'{{Activity.Type}}' = 'EmailSend' AND '{{Activity.Campaign.Id}}' = '"+str(campaign_id)+"'",
        }
        return self.build_export(bulk_api_url,data)

    def get_click_data(self):
        """Método para buscar todos os dados de clique  

            Parameters
            ----------       
            Returns
            -------
            list
                lista contendo todos os daos de clique
        """
        bulk_api_url = self.get_bulk_url()
        bulk_response = self.build_bounce(bulk_api_url)
        click_uri = str(bulk_response["uri"])
        syc_response = self.syc_data(bulk_api_url,click_uri)
        data_uri = syc_response["uri"]
        return self.get_data(bulk_api_url,data_uri)


    def get_bounce_data(self):
        """Método para buscar todos os dados de bounce  

            Parameters
            ----------       
            Returns
            -------
            list
                lista contendo todos os dados de bounce
        """
        bulk_api_url = self.get_bulk_url()
        bulk_response = self.build_bounce(bulk_api_url)
        build_uri = str(bulk_response["uri"])
        syc_response = self.syc_data(bulk_api_url,build_uri)
        data_uri = syc_response["uri"]
        return self.get_data(bulk_api_url,data_uri)

    def get_sent_data(self,campaign_id):
        """Método para buscar todos os dados de emials enviados de uma dada campanha  

            Parameters
            ----------  
            campaign_id : str
                string contendo o código de campnha 
            Returns
            -------
            list
                lista contendo todos os dados de email enviado daquela campanha
        """
        bulk_api_url = self.get_bulk_url()
        bulk_response = self.build_sent(bulk_api_url,campaign_id)
        build_uri = str(bulk_response["uri"])
        syc_response = self.syc_data(bulk_api_url,build_uri)
        data_uri = syc_response["uri"]
        return self.get_data(bulk_api_url,data_uri)

    def get_campaigns_sent(self,campaign_ids):
        """Método para buscar todos os dados de emials enviados de uma dada campanha  

            Parameters
            ----------  
            campaign_ids : list
                lista de todos os ids de campanha 
            Returns
            -------
            list
                lista contendo todos os dados de email enviado de todas as campanhas
        """
        data = []
        count = 1
        for campaign in campaign_ids:
            print("Retirando campanha de numero {} - ID: {}".format(count,campaign))
            count +=1
            data.extend(self.get_sent_data(campaign))
        return data
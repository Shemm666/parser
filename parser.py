import os
import socket
import telepot
import requests
from bs4 import BeautifulSoup
#import certifi
import pandas as pd
import selenium
import telebot
from threading import Thread
from selenium import webdriver
import configparser
from time import sleep
import geckodriver_autoinstaller
import urllib3
import json
from datetime import datetime as dt
from datetime import timedelta
from collections import defaultdict
geckodriver_autoinstaller.install() 
from selenium.webdriver.firefox.options import Options
from telepot import Bot
config = configparser.ConfigParser()
config.read("/home/user/parser/conf.ini")
search_url='https://goszakupki.by/tenders/posted?TendersSearch%5Bnum%5D=&TendersSearch%5Btext%5D={}&TendersSearch%5Bunp%5D=&TendersSearch%5Bcustomer_text%5D=&TendersSearch%5BunpParticipant%5D=&TendersSearch%5Bparticipant_text%5D=&TendersSearch%5Bprice_from%5D=&TendersSearch%5Bprice_to%5D=&TendersSearch%5Bcreated_from%5D=&TendersSearch%5Bcreated_to%5D=&TendersSearch%5Brequest_end_from%5D=&TendersSearch%5Brequest_end_to%5D=&TendersSearch%5Bauction_date_from%5D=&TendersSearch%5Bauction_date_to%5D=&TendersSearch%5Bindustry%5D={}&TendersSearch%5Btype%5D=&TendersSearch%5Bstatus%5D=&TendersSearch%5Bstatus%5D%5B%5D=Submission&TendersSearch%5Bstatus%5D%5B%5D=WaitingForBargain&TendersSearch%5Bstatus%5D%5B%5D=Examination&TendersSearch%5Bstatus%5D%5B%5D=Bargain&TendersSearch%5Bstatus%5D%5B%5D=Quantification&TendersSearch%5Bstatus%5D%5B%5D=Paused&TendersSearch%5Bstatus%5D%5B%5D=Signing&TendersSearch%5Bstatus%5D%5B%5D=Preselection&TendersSearch%5Bstatus%5D%5B%5D=DocsApproval&TendersSearch%5Bregion%5D=&TendersSearch%5Bappeal%5D=&page='
ch_bot_token=config['telegram_params']['ch_bot_token']
bot_token=config['telegram_params']['main_bot_token']
users_to_send=[u.strip() for u in config['telegram_params']['users_to_send'].split(',')]
options=Options()
options.add_argument("--headless")
driver = selenium.webdriver.Firefox(options=options)
driver2=selenium.webdriver.Firefox(options=options)
telegrambot=Bot(bot_token)
from selenium.common.exceptions import TimeoutException

# def soup_from_url(url):
#     driver.get(url)
#     return BeautifulSoup(driver.page_source)

# def link_gen(url):
#     for i in range(int(soup_from_url(url+'1').find('li',class_="last").text)):
#         yield url+str(i)


#search_dic={0:['прогр', 'систем'], 1:[[229,230,231,232,233,234,235,236,237,238,239,240,278,279,280,281,282,283,284,285,286,287,288,289]]}


# def get_parsed(format_key, value, search_url=search_url):
#     i=0
#     voted_new=[]
#     stop=False
#     format_params_dic={0:'',1:''}
#     value= '%'.join(str(value.encode("utf-8"))[2:-1].upper().split('\\X')) if format_key==0 else '%2C'.join(map(str, value))
#     format_params_dic[format_key]=value
#     base_url=search_url.format(format_params_dic[0], format_params_dic[1])
    
#     for i in range(1, int(soup_from_url(base_url).find('li',class_="last").text)+1):        
        
#         search_url=base_url+str(i)
#         soup=soup_from_url(search_url)
        
#         for parent in soup.find_all('tr')[1:]:
#             child=parent.find_all('td')            
#             voted_new.append([text.text for text in child]+[parent.find('a').text]+['https://goszakupki.by/'+parent.find('a')['href']])            
#     return pd.DataFrame(voted_new, columns=['№', 'Заявитель', 'Вид закупки', 'стадия', 'срок подачи','сумма','предмет закупки' ,'http' ])
    
        
url = 'http://goszakupki.by/tenders/posted?page='


class ParseThread(Thread):
    """класс отвечающий за парсинг данных о новых аукционах в отдельном потоке и выводе их в телеграм бот
    search_url-форматированая строка запроса на поиск, параметры форматирования изменяемые параметры поиска
    story-путь к файлу для хранения истории о запросах которые были выведены
    driver-driver brousera запуск в режиме headless
    bot-объект телеграмм бота
    search_dic-словарь параметров поиска
    u_id-список id телеграмм юзеров
    sleep_time  время в секундах между запросами"""
    def __init__(self,search_url, story, driver, bot,  sleep_time, rivals_json, ch_json):        
        Thread.__init__(self)
        self.cururl = None
        self.search_url=search_url
        self.story=pd.read_csv(story)
        self.driver=driver        
        self.bot=bot        
        self.sleep_time=sleep_time
        self.rivals_json=rivals_json        
        self.stop_plan=int(config['pur_plan_stop']['stop_plan'])        
        if os.path.exists(rivals_json):
            with open(rivals_json) as f:
                    d=json.load(f)
        else:
            d={}
        self.rivals_dict=defaultdict(dict, d)
        self.ch_json_name=ch_json
        
    def purch_plan_parse(self, organisations):
        """парсер планов по закупкам
        organisations-список ключевых слов для поиска по организациям"""
        global config
        stop=False
        page=1
        search_res_list=[]
        search_word=[word.strip() for word in config['search_params']['key_words'].split(',')]
        url='https://goszakupki.by{}'
        users_to_send=[u.strip() for u in config['telegram_params']['users_to_send'].split(',')]
        while not stop:
            soup=self.soup_from_url(url.format('/purchases/all?page={}'.format(page))).find('tbody').find_all('tr') 
            
            for item in soup:
                
                if int(item['data-key'])<self.stop_plan:
                    
                    stop=True
                    break
                elif any([org in item.find_all('td')[1].text for org in organisations]):
                    search_res_list.append((item['data-key'], item.find_all('td')[1].text, item.find('a')['href']))

            page+=1
        for _, org, href in search_res_list:
            soup=self.soup_from_url(url.format(href))
            if any([word in text for text in [teg.text for teg in soup.find_all('td')[1:]] for word in search_word]):
                self.send_mess(users_to_send, 'Соответствие в плане закупок для {}'.format(url.format(href)))            
        
        stop_plan=max(search_res_list, key=lambda x: x[0])[0]
        self.stop_plan=int(stop_plan)
        config.set('pur_plan_stop', 'stop_plan', str(stop_plan))
        with open('/home/user/parser/conf.ini', 'w') as configfile:
            config.write(configfile)
        
        
        
    def soup_from_url(self, url):
        driver.get(url)
        return BeautifulSoup(self.driver.page_source, features="html.parser")    
    
    def get_parsed(self, format_key, value):        
        i=0
        voted_new=[]
        stop=False
        format_params_dic={0:'',1:''}
        value= '%'.join(str(value.encode("utf-8"))[2:-1].upper().split('\\X')) if format_key==0 else '%2C'.join(map(str, value))
        format_params_dic[format_key]=value
        base_url=self.search_url.format(format_params_dic[0], format_params_dic[1])
        for i in range(1, int(self.soup_from_url(base_url).find('li',class_="last").text)+1):
            search_url=base_url+str(i)
            soup=self.soup_from_url(search_url)
            for parent in soup.find_all('tr')[1:]:
                child=parent.find_all('td')            
                voted_new.append([text.text for text in child]+[parent.find('a').text]+['https://goszakupki.by/'+parent.find('a')['href']])            
        return pd.DataFrame(voted_new, columns=['№', 'Заявитель', 'Вид закупки', 'стадия', 'срок подачи','сумма','предмет закупки' ,'http' ])
    
    def send_mess(self,u_id, msg):
        for id_ in u_id:
            self.bot.sendMessage(id_, msg)
            
    def observe_rivals(self, rival):
        rivals_result=pd.DataFrame(columns=['№','Заявитель', 'Предмет закупки', 'http'])
        result_list=[]        
        stop=False           
        rival_stop=self.rivals_dict[rival]
        i=1
        url='https://goszakupki.by/tenders/posted?TendersSearch[unpParticipant]={}'.format(rival)
        soup=self.soup_from_url(url)
        last_page=int(soup.find('li',class_="last").text) if soup.find('li',class_="last") else 1
        while i<=last_page:
            page_soup=self.soup_from_url(url+'&page={}'.format(str(i))).find_all('tr')[1:]
            for auc in page_soup:
                if auc.find('td').text!=rival_stop:
                    result_list.append([auc.find_all('td')[0].text, auc.find_all('td')[1].text, auc.find('a').text, 'https://goszakupki.by{}'.format(auc.find('a')['href'])])
                else:
                    stop=True            
                    break
            if stop:        
                break
            i+=1
        rivals_result=rivals_result.append(pd.DataFrame(result_list, columns=rivals_result.columns))
        
        return rivals_result, result_list
    
    def run(self):
        while True:
            global config
            key_words_list=[word.strip() for word in config['search_params']['key_words'].split(',')]
            topics_list=[[topic.strip() for topic in config['search_params']['topics'].split(',')]]
            search_dic={}
            search_dic[0]=key_words_list
            search_dic[1]=topics_list
            users_to_send=config['telegram_params']['users_to_send'].split(',')
            organisations=[org.strip() for org in config['organisations']['organisations'].split(',')]
            rivals=[rival.strip() for rival in config['rivals']['rivals'].split(',')]
            if os.path.exists(self.ch_json_name):
                with open(self.ch_json_name) as f:
                    ch_dict=json.load(f)
            else:        
                ch_dict=defaultdict(0)
            search_result_df=pd.DataFrame(columns=['№', 'Заявитель', 'Вид закупки', 'стадия', 'срок подачи', 'сумма','предмет закупки' ,'http' ])
            try:
                for key, value in search_dic.items():
                    k=key
                    for sub_val in value:
                        search_result_df=search_result_df.append(self.get_parsed(key, sub_val)) 
                
                search_result_df.drop_duplicates(subset=['№'], inplace=True)                
                search_result_df=search_result_df.loc[~search_result_df['№'].isin(self.story['№'].tolist()), :].reset_index(drop=True)
                
                for i in range(len(search_result_df)):
                    self.send_mess(users_to_send, search_result_df.iloc[i, :].to_string())
                    self.story=self.story.append(search_result_df.iloc[i, :])
                
                self.purch_plan_parse(organisations)
                for rival in rivals:
                    rivals_result, result_list=self.observe_rivals(rival)
                    if len(rivals_result):
                        rivals_result=rivals_result[~rivals_result['№'].isin(ch_dict.keys())].reset_index(drop=True)                       
                        for i in range(len(rivals_result)):
                            self.send_mess(users_to_send, 'УНП {} участвует в неотслеживаемом аукционе'.format(rival))
                            self.send_mess(users_to_send, rivals_result.iloc[i, :].to_string())                        
                        self.rivals_dict[rival]=result_list[0][0]
                        with open(self.rivals_json, 'w') as fp:
                            json.dump(self.rivals_dict, fp, ensure_ascii=False)
                            
                        
                    
            except TimeoutException:
                print('e')
                next
            except urllib3.exceptions.ReadTimeoutError:
                pass
            except socket.timeout:
                time.sleep(5) 
                print('socket.timeout')
            finally:
                self.story=self.story.iloc[0 if len(self.story)-100000<0 else len(self.story)-100000:,:].reset_index(drop=True)
                self.story.to_csv('story.csv', index=False)
            sleep(self.sleep_time)
        
        
class GetMess(Thread):
    """Номер интересующего аукциона принимается обработчиком сообщений
    бота и сохраняется в сет интересующих аукционов"""
    def __init__(self, token):
        Thread.__init__(self)
        
        self.bot=telebot.TeleBot(bot_token)        
        @self.bot.message_handler(content_types=["text"])        
        def text(message):
            global set_to_observe
            set_to_observe.add(message.text)
            print(set_to_observe)

    def run(self):
        self.bot.polling(none_stop=True, timeout=100)
        

class ChangeControl(Thread):
    """Класс для отслеживания изменений в интересующих аукционах, сохранения документов"""
    def __init__(self, dic, ch_bot_token, driver):
        """dic путь к json файлу хронящему статус при последнем проходе
        ch_bot_token-токен телеграмм бота отсылающего сообщения о изменениях в статусе отслеживаемых аукционов
        driver-driver brousera запуск в режиме headless
        u_id-список id телеграмм юзеров"""
        Thread.__init__(self)
        self.sleep_till=dt.now()
        if os.path.exists(dic):
            with open(dic) as f:
                d=json.load(f)
        else:
            d={}
        self.ddict=defaultdict(dict, d)
        self.bot=Bot(ch_bot_token)
        self.dictname=dic        
        self.driver=driver
        
        
        
    def soup_from_url(self, url):
        self.driver.get(url)
        soup=BeautifulSoup(self.driver.page_source, features="html.parser")
        
        return soup
    
    def get_state(self, soup, ddict, auc):
        """актуальное состояние интересующего аукциона"""
        try:
            soup_detailed=self.soup_from_url('https://goszakupki.by'+soup.find_all('tr')[1].find('a')['href'])
            ddict[auc]['Дата окончания приема предложений']=soup_detailed.find_all('td')[10].text
            ddict[auc]['Аукционные документы']=[elem.text for elem in soup_detailed.find_all('a', {'class':'modal-link'})]
            parent=soup_detailed.find('table', {'id':'lotsList'})
            descr=[lot.text for lot in parent.find_all('td', {'class':'lot-description'})]
            state=[lot.text for lot in parent.find_all('td', {'class':'lot-status'})]
            terms=[lot.next.next.next.text for lot in parent.find_all('b',{'class':'col-md-6'}) if lot.text=='Срок поставки:']
            for de, st, te in zip(descr, state, terms):
                ddict[auc][de]={'статус':st, 'сроки поставки':te}
            if not self.ddict[auc].get('docs_saved'):
                ddict[auc]['docs_saved']=False
            else:
                ddict[auc]['docs_saved']=self.ddict[auc]['docs_saved']
        except:
            print(auc)
            print(soup)
        return ddict, soup_detailed
    
    def save_json(self, ddict, name):
        with open(self.dictname, 'w') as fp:
            json.dump(ddict, fp, ensure_ascii=False)
    
    def is_closed(self, soup):        
        return soup.find('span', {'class':"badge"}).text=='Завершен' 
    
    def send_mess(self,u_id, msg):
        for id_ in u_id:
            self.bot.sendMessage(id_, msg)
            
    def save_file(self, ref, name, path):
        with open(path+'/'+name, "wb") as file:       
            response = requests.get('https://goszakupki.by'+ref, headers={"User-Agent":"Mozilla/5.0"})      
            file.write(response.content)
    
    def get_docs(self, soup_detailed, auc):        
        href=[ref['href'] for ref in soup_detailed.find_all('a') if ref.text.strip()=='Предложения участников размещены в открытом доступе']
        if href:
            oparts=self.soup_from_url('https://goszakupki.by'+href[0])
            partisip=oparts.find_all('div', "panel-body")
            for partisipant in partisip:
                links=[(href['href'], href.text )for href in partisipant.find_all('a') if 'qualification' in href['href']]
                if links:
                    os.makedirs(auc, exist_ok=True)
                    name=partisipant.next.strip()
                    directory=auc+'/'+name
                    os.makedirs(directory, exist_ok=True)
                    for link in links:
                        self.save_file(*link, directory)
                    self.send_mess(self.u_id, 'сохранены документы аукцион {} {} участник {}'.format(auc, 'https://goszakupki.by'+href[0], name))
            self.ddict[auc]['docs_saved']=True
                    
        
            
    def send_diff(self, u_id, old_dict, new_dict):
        for key in old_dict:
            if old_dict[key]!=new_dict[key]:
                self.send_mess(u_id, key)
                self.send_mess(u_id, json.dumps(old_dict[key], ensure_ascii=False))
                self.send_mess(u_id, 'на')
                self.send_mess(u_id, json.dumps(new_dict.get(key), ensure_ascii=False))
        if old_dict.keys()-new_dict.keys():
            self.send_mess(u_id, 'Убрано')
            for key in old_dict.keys()-new_dict.keys():
                self.send_mess(u_id, key)
                self.send_mess(u_id, json.dumps(old_dict[key], ensure_ascii=False))
        if new_dict.keys()-old_dict.keys():
            self.send_mess(u_id, 'Добавлено')
            for key in new_dict.keys()-old_dict.keys():
                self.send_mess(u_id, key)
                self.send_mess(u_id, json.dumps(new_dict[key], ensure_ascii=False))   
            
    def run(self):
        global set_to_observe        
        global config
        users_to_send=config['telegram_params']['users_to_send'].split(',')
        while True:
            try:
                while set_to_observe:
                    auc=set_to_observe.pop()
                    soup=self.soup_from_url('https://goszakupki.by/tenders/posted?TendersSearch[num]=auc'+auc)
                    self.get_state(soup, self.ddict, auc)
                    self.save_json(self.ddict, self.dictname)
                if dt.now()>self.sleep_till:
                    iter_l=list(self.ddict.keys())
                    for auc in iter_l:
                        soup=self.soup_from_url('https://goszakupki.by/tenders/posted?TendersSearch[num]=auc'+auc)                    
                        cur_state, soup_detailed=self.get_state(soup, defaultdict(dict), auc)
                        if cur_state[auc]!=self.ddict[auc]:
                            title=soup.find_all('tr')[1].find('a').text
                            self.send_mess(users_to_send, 'Изменено auc{}-{} {}'.format(auc, title, 'https://goszakupki.by/tenders/posted?TendersSearch[num]=auc'+auc))
                            self.send_diff(users_to_send, self.ddict[auc], cur_state[auc])
                            self.ddict[auc]=cur_state[auc]
                            self.save_json(self.ddict, self.dictname)
                        if not self.ddict[auc]['docs_saved']:
                            self.get_docs(soup_detailed, auc)
                        if self.is_closed(soup):
                            del self.ddict[auc]
                            self.save_json(self.ddict, self.dictname)
            except socket.timeout:                
                print('socket.timeout')
            except urllib3.exceptions.ReadTimeoutError:
                pass
            self.sleep_till=self.sleep_till+timedelta(minutes=3)
            sleep(5)
            

set_to_observe=set()
cc=ChangeControl('jdict.json', ch_bot_token, driver2)
getm=GetMess(bot_token)
getm.start()
cc.start()
auction_parse_thread=ParseThread(search_url, 'story.csv', driver, telegrambot,  600, 'rivals.json', 'jdict.json')
auction_parse_thread.start()

while True:    
    if not cc.is_alive():
        print (cc.is_alive)
        del cc
        cc=ChangeControl('jdict.json', ch_bot_token, driver2)
        cc.start()
    if not getm.is_alive():
        print(getm.is_alive)
        del getm
        getm=GetMess(bot_token)
        getm.start()
    if not auction_parse_thread.is_alive():
        print(auction_parse_thread.is_alive)
        del auction_parse_thread        
        auction_parse_thread=ParseThread(search_url, 'story.csv', driver, telegrambot,  600, 'rivals.json', 'jdict.json')
        auction_parse_thread.start()
    sleep(60)
    config.read("conf.ini")


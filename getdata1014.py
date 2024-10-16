# encoding:utf-8
import os
import time
import chardet
import logging
import requests
import pandas as pd
from lxml import etree
from urllib.parse import urlparse
from collections import deque,defaultdict
from typing import List,Dict,Any,Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pipeline = {
    'dlt':{
        'nodes':[
            {'id':'dlt','url':'https://data.17500.cn/dlt_asc.txt',
             'func':'fetch_parse_a','output_val':[],
             'save_conf':{'path':'dlt_data.csv','colname':['index','time','r1','r2','r3','r4','r5','b1','b2']}}
        ],
        'links':[]
    },
    'ssq':{
        'nodes':[
            {'id':'ssq','url':'https://data.17500.cn/ssq_asc.txt',
             'func':'fetch_parse_a','output_val':[],
             'save_conf':{'path':'ssq_data.csv','colname':['index','time','r1','r2','r3','r4','r5','r6','b1']}}
        ],
        'links':[]        
    },
    'kl8':{
        'nodes':[
            {'id':'kl8','url':'https://data.17500.cn/kl8_asc.txt',
             'func':'fetch_parse_a','output_val':[],
             'save_conf':{'path':'kl8_data.csv','colname':['index','time','r1','r2','r3','r4','r5','r6','r7',
            'r8','r9','r10','r11','r12','r13','r14','r15','r16','r17','r18','r19','r20']}}
        ],
        'links':[]        
    },
    'pl3':{
        'nodes':[
            {'id':'pl3','url':'https://data.17500.cn/pl3_asc.txt',
             'func':'fetch_parse_a','output_val':[],
             'save_conf':{'path':'pl3_data.csv','colname':['index','time','r1','r2','r3']}}
        ],
        'links':[]        
    },
    'pl5':{
        'nodes':[
            {'id':'pl5','url':'https://data.17500.cn/pl5_asc.txt',
             'func':'fetch_parse_a','output_val':[],
             'save_conf':{'path':'pl5_data.csv','colname':['index','time','r1','r2','r3','r4','r5']}}
        ],
        'links':[]
    },
    'foot_his':{
        'nodes':[
            {'id':'foot_his','url':'https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?gameNo=90&provinceId=0&&isVerify=1&pageSize=30&pageNo=',
             'func':'fetch_parse_b','output_val':[],
             'save_conf':{'path':'foot_his_data.csv','colname':['index','time','r1','r2','r3','r4','r5','r6','r7',
            'r8','r9','r10','r11','r12','r13','r14']}}
        ],
        'links':[]
    },
    'foot_new':{
        'nodes':[
            {'id':'issueid','url':'https://cp.zgzcw.com/lottery/getissue.action?lotteryId=300&issueLen=20',
             'func':'fetch_parse_c','output_val':[]},
            {'id':'matchlist','url':'https://cp.zgzcw.com/lottery/zcplayvs.action?lotteryId=13&issue=',
             'func':'fetch_parse_d','output_val':[]},
            {'id':'bjop','url':'https://fenxi.zgzcw.com/?playid?/bjop',
             'func':'fetch_parse_e','output_val':[]},
            {'id':'sink','url':'','func':'fetch_parse_f','output_val':[],
             'save_conf':{'path':'footnewinfo__data.csv','colname':[]}},
        ],
        'links':[{'from':'issueid','to':'matchlist'},
                 {'from':'matchlist','to':'bjop'},
                 {'from':'bjop','to':'sink'}]
    },
}

class Fetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60',
            'Accept':'application/json,text/html,*/*;q=0.9',
            'Accept-Language':'en-us,en;q=0.9',
            'Connection': 'keep-alive'
        })

    def fetch_url(self,url:str,response_type:str,timeout:int=10)->Any:
        parsed_url = urlparse(url)
        self.session.headers.update({'Referer':f"{parsed_url.scheme}://{parsed_url.netloc}"})
        try:
            logger.info(f"Fetching URL:{url}")
            response = self.session.get(url,timeout=timeout)
            time.sleep(6)
            response.encoding = chardet.detect(response.content)['encoding']
            response.raise_for_status()
            return response.json() if response_type =='json' else response.text
        except requests.Timeout:
            logger.error(f"请求超时：{url}")
        except requests.RequestException as e:
            logger.error(f"请求发生错误:{e}")
        return None

class Parser:
    def __init__(self) -> None:
        pass
    def get_siblings_content(self,td_element):
        return [''.join(sibling.itertext()).strip() for sibling in td_element.itersiblings()]
    def find_td_content(self,tree,text):
        td_element = tree.xpath(f'//td[@class="border-r border-l" and text()="{text}"]')
        if td_element:
            siblings_content = self.get_siblings_content(td_element[0])
            return [item for item in siblings_content if item and '-' not in item]
        else:
            print(f'未找到元素')

    def find_span_content(self,tree):
        span_element = tree.xpath(f'//span[@class="otherodds" and contains(text(),"离散度")]')
        if span_element:
            parts = span_element[0].text.split('|')
            # 去除每部分前后的空格，并分割成列表  
            lsd_values = [float(num) for num in parts[0].strip().split()[1:]]  
            fc_values = [float(num) for num in parts[1].strip().split()[1:]]  
            
            return {'lsd': lsd_values, 'fc': fc_values}
        else:
            print(f'未找到元素')

    def create_multiindex_df(self,data,columns,indexname):
        df = pd.DataFrame(data,index=indexname,columns=columns)
        new_columns = pd.MultiIndex.from_product([df.columns,indexname])
        new_df = pd.DataFrame(columns=new_columns)
        for idx,index in enumerate(indexname):
            new_df.loc[0,(slice(None),index)] = df.iloc[idx].values
        return new_df
            
class SNode:
    def __init__(self,id:str,url:str,func:str,output_val=None,save_conf:Optional[Dict[str,Any]]=None):
        self.id = id
        self.url = url
        self.pfunc = func
        self.input = []
        self.output = output_val
        self.path = save_conf.get('path') if save_conf else None
        self.colname =  save_conf.get('colname') if save_conf else None
    
    def execute(self):
        func = getattr(self,self.pfunc)
        func(self.input,self.url)

    def fetch_parse_a(self,input,url:str):
        '''get input +combine url'''
        data = fetcher.fetch_url(url,'text')
        len_sp = len(self.colname)
        if data:
            str_li = data.strip().split('\n')
            for str_line in str_li:
                sp_li = str_line.split()[:len_sp]
                self.output.append(sp_li)
            self.save_data(self.output)

    def fetch_parse_b(self,input,baseurl:str):
        local_df = self.get_local_df()
        local_li = local_df.values.tolist() if local_df is not None else []
        need_update = True
        numpage = 1
        while need_update:
            url = f"{baseurl}{numpage}"
            data = fetcher.fetch_url(url,'json')
            numpage += 1
            if not data or numpage > data['value']['pages']:
                need_update = False
                break                  
            for resitem in data['value']['list']:
                data_string = [resitem['lotteryDrawNum'],resitem['lotteryDrawTime']] + \
                                resitem['lotteryDrawResult'].split()
                if local_df is None or int(data_string[0]) > int(local_df.iloc[0,0]):
                    self.output.append(data_string)
                else:
                    need_update =False
                    break
        self.output += local_li
        self.save_data(self.output)        

    def fetch_parse_c(self,input,url:str):
        '''get input +combine url / parse data + output'''
        data = fetcher.fetch_url(url,'json')
        if data:
            for item in data:
                match_dict = {'issueid':item['issue'],'startTime':item['startTime'],
                             'endTime':item['endTime'],'leftTime':item['leftTime']}
                self.output.append(match_dict)

    def fetch_parse_d(self,input,baseurl:str):
        '''get input +combine url / parse data + output'''
        next_match_li = input
        for match_id in next_match_li:
            url = f"{baseurl}{match_id['issueid']}"
            data = fetcher.fetch_url(url,'json')
            if data:
                for item in data['matchInfo']:
                    self.output.append({'issueid':match_id['issueid'],'playid':item['playId'],'leage':item['leageName'],
                             'host':item['hostNameFull'],'guest':item['guestNameFull']})        

    def fetch_parse_e(self,input,baseurl:str):
        '''get input +combine url / parse data + output'''
        next_info_dict_li = input
        next_info_df = pd.DataFrame()
        for dict_item in next_info_dict_li:
            url = baseurl.replace('?playid?',dict_item['playid'])
            data = fetcher.fetch_url(url,'text')
            if data:
                index_df0 = pd.DataFrame([dict_item])

                tree = etree.HTML(data)
                meanli = parser.find_td_content(tree,'平均值')
                maxli = parser.find_td_content(tree,'最大值')
                minli = parser.find_td_content(tree,'最小值')
                columns = ['old_ods_w','old_ods_d','old_ods_d','new_ods_w','new_ods_d','new_ods_l',
                'w_p','d_p','l_p','kl_w','kl_d','kl_l','ex_r']
                indexname = ['min','mean','max']
                multi_df1 = parser.create_multiindex_df([minli,meanli,maxli],columns,indexname)

                res_dict = parser.find_span_content(tree)
                columns = ['lsd','fc']
                indexname = ['klw','kld','kll']                    
                multi_df2 = parser.create_multiindex_df(res_dict,columns,indexname)
                
                multi_df = pd.concat([index_df0,multi_df1,multi_df2],axis=1)

                next_info_df = pd.concat([next_info_df,multi_df],axis=0)
        self.output = [next_info_df]
                
    def fetch_parse_f(self,input,baseurl:str):
        next_info_df = input[0]
        self.save_data(next_info_df)

    def get_local_df(self):
        if self.path and os.path.isfile(self.path):
            return pd.read_csv(self.path)
    
    def save_data(self,in_data):
        if isinstance(in_data,list):
            if self.colname and self.path:
                df = pd.DataFrame(in_data,columns=self.colname)
                df.to_csv(self.path,index=False)
        elif isinstance(in_data,pd.DataFrame):
            in_data.to_csv(self.path,index=False)

class Graph:
    def __init__(self) -> None:
        self.graph = defaultdict(list)
        self.indegree = defaultdict(int)
    def add_link(self,u:SNode,v:SNode):
        self.graph[u].append(v)
        self.indegree[v] += 1
        self.indegree.setdefault(u,0)

    def bfs(self):
        queue = deque(node for node in self.indegree if self.indegree[node] == 0)
        result = []
        while queue:
            node = queue.popleft()
            result.append(node)
            node.execute()
            for neighbor in self.graph[node]:
                neighbor.input.extend(output for output in node.output if output not in neighbor.input)
                self.indegree[neighbor] -= 1
                if self.indegree[neighbor] == 0:
                    queue.append(neighbor)
        return result

def init_fetcher_parser():
    global fetcher,parser
    fetcher = Fetcher()
    parser = Parser()

def load_graph_from_config(config:Dict[str,Any]):
    g = Graph()
    nodes = {node_config['id']:SNode(**node_config) for node_config in config['nodes']}
    for node in nodes.values():
        g.indegree[node]
    for link in config['links']:
        g.add_link(nodes[link['from']],nodes[link['to']])
    return g

if __name__ == "__main__":
    init_fetcher_parser()
    for key,value in pipeline.items():
        g = load_graph_from_config(value)
        if len(g.indegree) == 1:
            single_node = next(iter(g.indegree))
            single_node.execute()
            #logger.info(f'Output:{single_node.output}')
            logger.info(f'Node Id:{single_node.id}')
        else:
            result = g.bfs()
            for node in result:
                #logger.info(f'Input:{node.input},Output:{node.output}')
                logger.info(f'Node Id:{node.id}')

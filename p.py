#!/usr/bin/env python
# -*- coding: utf-8 -*-

############################################## LIBRARIES ######################################
import sys                                                                      
from xml.dom.minidom import parse, parseString
from xml.dom.minidom import Node, Element, Attr
import pandas as pd
import multiprocessing as mp
from contextlib import closing
from multiprocessing import Pool
#from neo4j.v1 import GraphDatabase
################################################################################################
dom = parse('patients.xml')
Doc=dom.getElementsByTagName('doc')
hash_table_pacients = dict()
r_list= list()
a_list = list()
columns = ["ID_PACIENT","DATE","TEXT"]
lista_type = []
lista_data= []
lista_relaciones=[]
list_CIM10=[]
list_CIAP2=[]
list_ATC=[]

class Diagnostico:
    def __init__(self):
        data = []
        columns = ["ID_NODE","ID_PACIENT","DATE","TIEMPO","CIAP2","CIM10","CERT","COUNT"]
        self.df = pd.DataFrame(data, columns = columns)

    def add_diag(self,id_node,id_patient,date,tiempo,cim10,cert):
        i= self.df.index[(self.df['TIEMPO']==tiempo) & (self.df['CIM10']==cim10) & (self.df['CERT']==cert)].tolist()
        if len(i)==1:
            self.df.loc[i[0],'COUNT'] +=1
            self.df.loc[i[0],'ID_NODE'].append(id_node)
        else:
            row= [[id_node],id_patient,date,tiempo,'',cim10,cert,1]
            self.df.loc[len(self.df)]= row

    def __print__(self):
        print ("QUE LADILLA")
        print (self.df)


class Farmaco:
    def __init__(self):
        data = []
        columns = ["ID_NODE","ID_PACIENT","DATE","ATC","CERT","TIEMPO","SIT","COUNT"]
        self.df = pd.DataFrame(data, columns = columns)

    def add_farm(self,id_node,id_patient,date,atc,tiempo,cert,sit):
        i= self.df.index[(self.df['ATC']==atc) & (self.df['TIEMPO']==tiempo) & (self.df['CERT']==cert) & (self.df['SIT']==sit)].tolist()
        if len(i)==1:
            self.df.loc[i[0],'COUNT'] +=1
            self.df.loc[i[0],'ID_NODE'].append(id_node)
        else:
            row= [[id_node],id_patient,date,atc,cert,tiempo,sit,1]
            self.df.loc[len(self.df)]= row

    def __print__(self):
        print ("QUE LADILLA")
        print (self.df)

class Signo_sintoma:
    def __init__(self):
        data = []
        columns = ["ID_PACIENT","DATE","data","CERT","TIEMPO","COUNT"]
        self.df = pd.DataFrame(data, columns = columns)

    def add_sig_sin(self,id_node,id_patient,date,data,cert,tiempo):
        i= self.df.index[(self.df['data']==data.lower()) & (self.df['TIEMPO']==tiempo) & (self.df['CERT']==cert)].tolist()
        if len(i)==1:
            self.df.loc[i[0],'COUNT'] +=1
            self.df.loc[i[0],'ID_NODE'].append(id_node)
        else:
            row= [[id_node],id_patient,date,data.lower(),cert,tiempo,1]
            self.df.loc[len(self.df)]= row

    def __print__(self):
        print ("QUE LADILLA")
        print (self.df)

class Variable:
    def __init__(self):
        data = []
        columns = ["ID_NODE","ID_PACIENT","DATE","data","CERT","TIEMPO","COUNT"]
        self.df = pd.DataFrame(data, columns = columns)

    def add_var(self,id_node,id_patient,date,data,cert,tiempo):
        i= self.df.index[(self.df['data']==data.lower()) & (self.df['TIEMPO']==tiempo) & (self.df['CERT']==cert)].tolist()
        if len(i)==1:
            self.df.loc[i[0],'COUNT'] +=1
            self.df.loc[i[0],'ID_NODE'].append(id_node)
        else:
            row= [[id_node],id_patient,date,data.lower(),cert,tiempo,1]
            self.df.loc[len(self.df)]= row

    def __print__(self):
        print ("QUE LADILLA")
        print (self.df)


class Analitica:
    def __init__(self):
        data = []
        columns = ["ID_NODE","ID_PACIENT","DATE","data","CERT","TIEMPO","COUNT"]
        self.df = pd.DataFrame(data, columns = columns)

    def add_analit(self,id_node,id_patient,date,data,cert,tiempo):
        i= self.df.index[(self.df['data']==data.lower()) & (self.df['TIEMPO']==tiempo) & (self.df['CERT']==cert)].tolist()
        if len(i)==1:
            self.df.loc[i[0],'COUNT'] +=1
            self.df.loc[i[0],'ID_NODE'].append(id_node)
        else:
            row= [[id_node],id_patient,date,data.lower(),cert,tiempo,1]
            self.df.loc[len(self.df)]= row

    def __print__(self):
        print ("QUE LADILLA")
        print (self.df)




class Resultado:
    def __init__(self):
        data = []
        columns = ["ID_NODE","ID_PACIENT","DATE","data","tipo","COUNT"]
        self.df = pd.DataFrame(data, columns = columns)

    def add_resul(self,id_node,id_patient,date,data,tipo):
        i= self.df.index[(self.df['data']==data.lower()) & (self.df['tipo']==tipo)].tolist()
        if len(i)==1:
            self.df.loc[i[0],'COUNT'] +=1
            self.df.loc[i[0],'ID_NODE'].append(id_node)
        else:
            row= [[id_node],id_patient,date,data.lower(),tipo,1]
            self.df.loc[len(self.df)]= row

    def __print__(self):
        print ("QUE LADILLA")
        print (self.df)




class parteCuerpo:
    def __init__(self):
        data = []
        columns = ["ID_NODE","ID_PACIENT","DATE","data","COUNT"]
        self.df = pd.DataFrame(data, columns = columns)


    def add_parteCuerpo(self,id_node,id_patient,date,data):
        #print ("HOLA")
        i= self.df.index[(self.df['data']==data.lower())].tolist()
        if len(i)==1:
            self.df.loc[i[0],'COUNT'] +=1
            self.df.loc[i[0],'ID_NODE'].append(id_node)
        else:
            row= [[id_node],id_patient,date,data.lower(),1]
            self.df.loc[len(self.df)]= row

    def __print__(self):
        print ("QUE LADILLA")
        print (self.df)










# ##########################################################################################################################################
# THIS STRUCTURE CONTAINS THE INFORMATION OF ONE PATIENTS. THIS IS A DICTIONARY THAT FOR EACH PATIENT(ID) CONTAINS THE CORRESPONDING GRAPH.#
# INPUT IS THE ID_PATIENT ( 1,2,3) AND THE GRAPH OF THIS PATIENT.                                                                          #
# INSERTION = O(1)                                                                                                                         #
# SEARCH = O (1)                                                                                                                           #
# ##########################################################################################################################################

class PAC:

    def __init__(self):
        self.array_graph=dict()

    def _add_patient_(self,ID_PACIENT,graph):
        self.array_graph[ID_PACIENT]= graph

# ##########################################################################################################################################
# NODE CONTAINS THE INFORMATION OF THE NODES OF THE DAG. NOT WEIGTH RIGH NOW                                 
# INPUT IS PATIENT INFORMATION ( 1,2,3) AND THE GRAPH OF THIS PATIENT.                                                                          #
# INSERTION = O(1)                                                                                                                         #
# SEARCH = ? ( need another structure)                                                                                                                           #
# ##########################################################################################################################################

class Node:
    def __init__(self,self_ident,_id,_type,TIEMPO,CERT,CIM10,CIAP2,ATC,SIT,VAR,TIPO,data,weigth=None):
        self.TIEMPO= TIEMPO
        self._ident= self_ident
        self.id= _id
        self._type= _type 
        self.CERT=CERT
        self.CIM10= CIM10
        self.CIAP2= CIAP2
        self.ATC=ATC
        self.SIT= SIT
        self.VAR= VAR
        self.TIPO= TIPO
        self.data= data
        self.weigth=0
        self.EDGE=list()
        #self.EDGE_destin=list()

    def _sum_weig_(self):
        self.weigth= self.weigth+1

    def add_edge_origin(self,destin):
        self.EDGE.append(destin)

    def __print__(self):
        print ("ID: "+str(self.id)+" . IDENT:" +str(self._ident)+ " . Type: "+self._type)
        print ("TIEMPO: "+self.TIEMPO+". CERT: "+self.CERT)
        print ("CIM10: "+self.CIM10+" . CIAP2: "+self.CIAP2+" . ATC: "+ self.ATC+ " . SIT: "+self.SIT+ ". VAR: "+self.VAR+ " . TIPO: "+ self.TIPO+" . data: "+self.data)
        print ("ORIGEN-DESTINO:")
        #for i in self.EDGE_origin:
        #    print i.__print__()
        #print ("ESTINO-EDGES:")
        #for i in self.EDGE_destin:
        #    print i.__print__()
class Edge:

    def __init__(self,origin,destin,name_relation):
        self.origin= origin
        self.destin= destin
        self.name_relation=name_relation 

    def __print__(self):
        print ("ORIGIN:"+self.origin+"DESTI:"+self.destin+"NAME_RELATION:"+self.name_relation)


# ##########################################################################################################################################
# DAG CONTAINS SEQUENCES OF OUR PROBLEM. THE IDEA PRINCIPAL IS TO HAVE SOMEHOW THE SEQUENCES. I ACTUALY HAVE THE NODE IN TWO WAYS. 
# CREATING THE NODES ( CLASS NODE) AND HAVEING THE PROBLEM LIKE A ITEMSET TRANSACTION.                             
# INSERTION:  
# SEARCH:                                                                                                                         #
# ##########################################################################################################################################

# just create something, but it is not correct. # vertices are missing. # better solution for vertices NEED IT.
class DAG:

    def __init__(self):
        self.numbers= list()
        self.nodes = dict();
        self.vertices = list();

    def _add_numbers_(self,n):
        self.numbers.append(n)

    def _add_node_(self,ID,node):
        self.nodes[ID]= node

    def _add_edge_(self,origin,destin,name_relation):
        e= Edge(origin,destin,name_relation)
        self.vertices.append(e) #this is really bad.
        self.nodes[origin].add_edge_origin(destin)

    def __print__(self):
        for i in self.numbers:
            self.nodes[i].__print__()

class Patient:
    def __init__(self,number,date,_type,text,annotation_list,relation_list):
        self.number= number
        self.date=date
        self.type=_type
        self.text= text
        self.annotation_list= annotation_list
        self.relation_list= relation_list

    def add_annotation_list(self,A):
        self.annotation_list.append(A)

    def add_relation_list(self,A):
        self.relation_list.append(A)

class Relation:
    def __init__(self,source,target,_type):
        self.source= source
        self.target= target
        self.type= _type

    def print__(self):
        print("SOURCE: "+self.source+" . TARGET:" +self.target+ " . TYPE: "+self.type)

class Annotation:
    def __init__(self, Id, Start, End, TIEMPO, Type, CERT, CIM10, CIAP2,ATC,SIT,VAR,TIPO,data):
        self.Id= Id
        self.Start= Start
        self.End= End
        self.TIEMPO= TIEMPO
        self.Type= Type
        self.CERT= CERT
       # print ("PRUEBA PARA VER QUE ESTA ENTRADNDO"+CIM10)
        self.CIM10= CIM10
        self.CIAP2= CIAP2
        self.ATC=ATC
        self.SIT= SIT
        self.VAR= VAR
        self.TIPO= TIPO
        self.data= data

    def print__(self):
        print ("ID: "+self.Id+" . Start:" +self.Start+ " . End: "+self.End)
        print ("TIEMPO: "+self.TIEMPO+" . Type:" +self.Type+ ". CERT: "+self.CERT)
        print ("CIM10: "+self.CIM10+" . CIAP2: "+self.CIAP2+" . ATC: "+ self.ATC+ " . SIT: "+self.SIT+ ". VAR: "+self.VAR+ " . TIPO: "+ self.TIPO+" . data: "+self.data)

def create_list(x,_list):
    if x in _list:
        print("ya existe")
    else:
        _list.append(x)
    return _list

def CreateListRelation(__type):
    if __type in lista_relaciones:
        print ("ya existe")
    else:
        lista_relaciones.append(__type)

def lista_argumentos(__type,data):
    if data in lista_data:
        print("ya existe")
    else:
        lista_data.append(data)
        lista_type.append(__type)

def lecture_xml(pac):
    i = 0
    qq=-1
    for node in Doc:
        date= node.getAttribute('date')
        patient= node.getAttribute('patient')
        _type= node.getAttribute('type')
        textList=node.getElementsByTagName('text')
        if (qq != int(patient)):
            dag = DAG()
            qq= int(patient)

        for a in textList:
                text= a.firstChild.data
        a_list =list()
        r_list= list()
        p1= Patient(node.getAttribute('patient'), node.getAttribute('date'),node.getAttribute('type'),text,a_list,r_list)
       
        annotationList = node.getElementsByTagName('Annotation')
        for a in annotationList:
            Id = a.getAttribute('Id')
            Start = a.getAttribute('Start')
            End = a.getAttribute('End')
            TIEMPO= a.getAttribute('TIEMPO')
            if TIEMPO =='':
                TIEMPO='A'
            _type = a.getAttribute('Type')
            CERT= a.getAttribute('CERT')
            if CERT =='':
                CERT='A'
            CIM10= a.getAttribute('CIM10')
            create_list(CIM10,list_CIM10)
            CIAP2= a.getAttribute('CIAP2')
            create_list(CIAP2,list_CIAP2)         
            ATC= a.getAttribute('ATC')
            create_list(ATC,list_ATC)
            SIT= a.getAttribute('SIT')
            if SIT =='':
                SIT='A'
            TIPO= a.getAttribute('TIPO')
            VAR= a.getAttribute('VAR')
            data = a.firstChild.data
            a1= Annotation(Id, Start, End, TIEMPO, _type, CERT, CIM10, CIAP2,ATC,SIT,VAR,TIPO,data)
            n_node=Node(i,Id,_type,TIEMPO,CERT,CIM10,CIAP2,ATC,SIT,VAR,TIPO,data,weigth=None)
            dag._add_node_(Id,n_node)
            dag._add_numbers_(Id)
            i=i+1
            lista_argumentos(_type,data)
            p1.add_annotation_list(a1)
            if _type=='Diagnostico':
                print ("OK ESTO ES UN DIAGNOSTICO")
                diag.add_diag(Id,patient,date,TIEMPO,CIM10,CERT)
            elif (_type=='Farmaco'):
                print ("OK ESTO ES UN FARMACO")
                farm.add_farm(Id,patient,date,ATC,TIEMPO,CERT,SIT)
            elif (_type=='Signo_sintoma'):
                sig.add_sig_sin(Id,patient,date,data,CERT,TIEMPO)
                print ("ES UN SIGNO Y SINTOMA")
            elif (_type=='Variable'):
                var.add_var(Id,patient,date,data,CERT,TIEMPO)
            elif (_type=='Analitica'):
                anali.add_analit(Id,patient,date,data,CERT,TIEMPO)
            elif (_type=='Resultado'):
                resultado.add_resul(Id,patient,date,data,TIPO)
            elif (_type=='ParteCuerpo'):
                parteCuerpo.add_parteCuerpo(Id,patient,date,data)
            else:
                print ""
        relationList = node.getElementsByTagName('Relation')
        for a in relationList:
            relationType = a.getAttribute('Type')
            relationTarget = a.getAttribute('Target')
            relationSource = a.getAttribute('Source')
            r1= Relation(relationSource,relationTarget,relationType)
            dag._add_edge_(relationSource,relationTarget,relationType)
            f.write(relationSource +" "+relationTarget+"\n")
            p1.add_relation_list(r1)
            CreateListRelation(relationType)
        pac._add_patient_(int(patient),dag)
        hash_table_pacients[patient,date]= p1
    diag.__print__()

def ConstructDictFromListaData(lista_data):
    dataDict = dict()
    j=0
    for i in lista_data:
        dataDict[i]=j
        j= j +1
    return dataDict

# importante dejar claroq ue el ID_NODE no es el mismo que el que tiene le nodo. esto debe verse
def CreateDataset(__indexationMin,__indexationMax):
    data = []
    columns = ["ID_NODE","ID_PACIENT","DATE","__type","TIEMPO_A","TIEMPO_P","CIAP2","CERT_A","CERT_N","CERT_P","SIT_A","SIT_B","TIPO_C","TIPO_N","TIPO_OTHER","data"]
    columns.extend(list_CIM10)
    columns.extend(list_ATC)
    df = pd.DataFrame(data, columns = columns)
    dataDictCIM10=ConstructDictFromListaData(list_CIM10)
    dataDictATC=ConstructDictFromListaData(list_ATC)

    temp=0
    ID=0
    for y in range(__indexationMin,__indexationMax):
        print("AJA, LA Y ES"+str(y))
        i = hash_table_pacients.values()[y]
        row=[]
        ID_PATIENT= i.number
        DATE= i.date
        for j in i.annotation_list:
            row=[]
            __type=j.Type
            TIEPO=j.TIEMPO
            CIAP2=j.CIAP2
            CIM10=j.CIM10
            CERT=j.CERT
            SIT=j.SIT
            ATC=j.ATC
            TYPO=j.TIPO
            DATA=j.data
            row.append(j.Id)
            row.append(i.number)
            row.append(i.date)
            row.append(j.Type)
            if (j.TIEMPO=='A'):
                row.append(1)
                row.append(0)
            elif (j.TIEMPO=='P'):
                row.append(0)
                row.append(1)
            else:
                row.append(1)
                row.append(0)
            row.append(j.CIAP2)
            if (j.CERT =='A'):
                row.append(1)
                row.append(0)
                row.append(0)
            elif (j.CERT=='N'):
                row.append(0)
                row.append(1)
                row.append(0)
            else:
                row.append(0)
                row.append(0)
                row.append(1)
            if (j.SIT =='A'):
                row.append(1)
                row.append(0)
            elif (j.SIT=='B'):
                row.append(0)
                row.append(1)
            else:
                row.append(1)
                row.append(0)
                
            if (j.TIPO =='C'):
                row.append(1)
                row.append(0)
                row.append(0)
            elif (j.TIPO=='N'):
                row.append(0)
                row.append(1)
                row.append(0)
            else:
                row.append(0)
                row.append(0)
                row.append(1)
            row.append(j.data)
            indexCIM10= dataDictCIM10[j.CIM10]
            indexATC= dataDictATC[j.ATC]
            print(indexCIM10)
            for t in range(16,indexCIM10 + indexATC+16):
                row.append(0)
            row.append(1)
            for t in range(indexCIM10 + indexATC + 17,len(list_CIM10)+ len(list_ATC)+ 16):
                row.append(0)
            df.loc[temp]= row
            temp = temp +1
            ID= ID +1 
    return df


f=open("kk.txt","w")
pac = PAC()
diag= Diagnostico()
farm= Farmaco()
sig=Signo_sintoma()
var =Variable()
anali= Analitica()
resultado= Resultado()
parteCuerpo= parteCuerpo()
lecture_xml(pac)
diag.__print__()
len_hash = len(hash_table_pacients)
print(len(list_CIM10))
print(len(list_CIAP2))
print(len(list_ATC))

with closing(Pool(processes = 26)) as pool:
    print ("Estoy aqui")
    r1 = pool.apply_async(CreateDataset,(0,20))
    r2 = pool.apply_async(CreateDataset,(20,40))
    r3 = pool.apply_async(CreateDataset,(40,60))
    r4 = pool.apply_async(CreateDataset,(60,80))
    r5 = pool.apply_async(CreateDataset,(80,150))
'''
    r6 = pool.apply_async(CreateDataset,(100,120))
    r7 = pool.apply_async(CreateDataset,(120,140))
    r8 = pool.apply_async(CreateDataset,(140,160))
    r9 = pool.apply_async(CreateDataset,(160,180))
    r10 = pool.apply_async(CreateDataset,(180,200))
    r11 = pool.apply_async(CreateDataset,(200,220))
    r12 = pool.apply_async(CreateDataset,(220,240))
    r13 = pool.apply_async(CreateDataset,(240,260))
    r14 = pool.apply_async(CreateDataset,(260,280))
    r15 = pool.apply_async(CreateDataset,(280,300))
    r16 = pool.apply_async(CreateDataset,(300,320))
    r17 = pool.apply_async(CreateDataset,(320,340))
    r18 = pool.apply_async(CreateDataset,(340,360))
    r19 = pool.apply_async(CreateDataset,(360,380))
    r20 = pool.apply_async(CreateDataset,(380,400))
    r21 = pool.apply_async(CreateDataset,(400,420))
    r22 = pool.apply_async(CreateDataset,(420,440))
    r23 = pool.apply_async(CreateDataset,(440,460))
    r24 = pool.apply_async(CreateDataset,(460,480))
    r25 = pool.apply_async(CreateDataset,(480,500))
    r26 = pool.apply_async(CreateDataset,(500,537))
'''

print("HE ACABO EL PPOL")
df1= r1.get()
df2= r2.get()
df3= r3.get()
df4= r4.get()
df5= r5.get()
#df6= r6.get()
print("TOO LSITO PARA LOS FEMES")
frames = [df1,df2,df3,df4,df5]  #df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20,df21,df22,df23,df24,df25,df26]
result = pd.concat(frames)
f.close()
#dag.__print__()
diag.__print__()
farm.__print__()
sig.__print__()
var.__print__()
anali.__print__()
resultado.__print__()
parteCuerpo.__print__()
result.to_csv('pacientes_natacha_final_nuevo.csv',index = None, header=True, encoding="utf-8")




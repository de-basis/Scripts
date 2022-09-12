import ms.version
ms.version.addpkg("ibm_db","3.0.4-11.1.6.1")
ms.version.addpkg("ms.db2","1.0.4.1")
ms.version.addpkg("ms.modulecmd","1.1.1")
import ms.db2
import ms.modulecmd
import logging
import datetime
ms.modulecmd.load("ibmdb2/client/11.1.5.1")

ag_file_path="/home/.."     # ADD ag file path
br_acc_file_path="/home/.." # ADD brach acc file path
br_keywords=['brach','office']     # ADD  branch office keywords
acc_keywords=['account','office']   # ADD account keywords

logging.basicConfig(filename="./og_file.log",
                    format='%(asctime)s %(message)s',datefmt='%b %d %Y %H %M %S')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
def run(query):
    connection=ms.db2.connect("dcmodwm2")
    cursor=connection.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    connection.close()
    return result

def read_br_acc(path):
    br_acc=[]
    file=open(path,'r',errors='ignore')
    lines=file.readlines()
    for line in lines:
        data=line.split(' ')
        data[0]=str(int(data[0]))
        data[1]=str(int(data[1]))
        br_acc.append(data[0]+data[1])
    return br_acc

def get_ag_info(ag_file_path):
    ag_str=""
    file=open(ag_file_path,'r',errors='ignore')
    ag_names=file.readlines()
    for ag_name in ag_names:
        ag_str=ag_str+"'"+ag_name.strip()+"',"
    ag_str=ag_str[:-1]
    ag_info=run("select name, agid, agid_name from odwmdev.arsag where name in ("+ag_str+")")
    params=[]
    all_params=[]
    for i in ag_info:
        params.append(list(i))
    logger.info(f"Extracted [AG_NAME, AGID, AGID_NAME] {params}")
    for i in params:
        fields=run("select name from odwmdev.ARSAGFLD where agid="+str(i[1]))
        field_names=[fld.lower() for tup in fields for fld in tup]
        for word in br_keywords:
            if word in field_names:
                br_field=word
                break
            else:
                br_field='NA'
        for word in acc_keywords:
            if word in field_names:
                acc_field=word
                break
            else:
                acc_field='NA'
        table_names=run("select tabname from odwmdev.syscat.tables where tabname like '"+i[2]+"%'")
        tables=[tab for tup in table_names for tab in tup]
        logger.info("AGID: {} fields: {} Tables: {}".format(str(i[1]),field_names,tables))
        try:
            if((br_field!='NA') and (acc_field!='NA')):
                all_params.append([i[0],i[1],i[2],tables,[br_field,acc_field],len(tables)])
            else:
                logger.info("Either Field Name is not in Keywords: {} {}".format(br_field,acc_field))
        except:
            pass
    #print(*all_params,sep='\n')
    logger.info("List containing all AG info: {}".format(all_params))
    return all_params

def lookup(ag_info,branch_accounts):
    findings=[]
    for i in ag_info:
        for table in i[3]:
            try:
                result=run("select distinct concat(cast(cast({} as INT) as VARCHAR),cast(cast({} as INT) as VARCHAR)),concat({},{}),date(loaddate) from odwmdev.{}".format(i[4][0],i[4][1],i[4][0],i[4][1],table))
                results=[tup for tup in result]
                for r in results:
                    if r[0] in branch_accounts:
                        findings.append([i[0],i[1],i[2],table,r[1],i[4],r[2]])
                logger.info("Querying table: {} found br+acc: {}".format(table,r[1]))
            except:
                logger.exception("br+acc not found")
            try:
                result=run("select distinct concat(cast(cast({} as INT) as VARCHAR),cast(cast({} as INT) as VARCHAR)),concat({},{}),date(load_date) from odwmdev.{}".format(i[4][0],i[4][1],i[4][0],i[4][1],table))
                results=[tup for tup in result]
                for r in results:
                    if r[0] in branch_accounts:
                        findings.append([i[0],i[1],i[2],table,r[1],i[4],r[2]])
                logger.info("Querying table: {} found br+acc: {}".format(table,r[1]))
            except:    
                logger.exception("br+acc not found")
                pass
    #print("********AG INFO********")
    #print(*findings, sep='\n')
    return findings

def count_occurrence(findings):
    #counts=[]
    ls=[]
    for i in findings:
        result=run("select concat({},{}),date(loaddate) from odwmdev.{} where concat({},{})={}".format(i[5][0],i[5][1],i[3],i[5][0],i[5][1],i[4]))
        results=[tup for tup in result]
        ls.extend(results)
        #counts.append([i[4],i[3],result[0][0],i[6]])
    return ls

def main():
    branch_accounts=read_br_acc(br_acc_file_path)
    ag_info=get_ag_info(ag_file_path)
    result=lookup(ag_info,branch_accounts)
    matches=count_occurrence(result)
    today=datetime.date.today()
    cnt_gr10=0
    cnt_ls10=0
    for i in matches:
        date_str=str(i[1])
        #print(type(i[1]),"  ",date_str)
        try:
            date1=datetime.date(int(date_str[:4]),int(date_str[5:7]),int(date_str[8:]))
            diff=today-date1
            years=(diff.days)/365
        except:
            print("Date format Different than Expected")
        if years>10:
            cnt_gr10+=1
        elif years<10:
            cnt_ls10+=1
    print("Count of Load Date > 10 years: ", cnt_gr10)       
    print("Count of Load Date < 10 years: ", cnt_ls10)     
if __name__==__main__:
    main()
        

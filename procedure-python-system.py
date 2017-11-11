import pymysql
import json

conn = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=DATABASE)

try:
    cursor = conn.cursor()
    with open('data.json') as data:
        a = json.load(data)
        for i in a:
            sql = "insert into tb_customer_account (cpf_cnpj, nm_customer, is_active, vl_total)\
                   values ('{}','{}',{},{:.2f})"\
                   .format(i['cpf_cnpj'], i['nm_customer'],
                           i['is_active'], i['vl_total']) #Inserindo os dados que foram recebidos de um
                                                          #arquivo json na tabela tb_costumer_account
            cursor.execute(sql)
        cursor.close()
        conn.commit()
except:
    print("Um erro inesperado ocorreu...")
    cursor.close()
    conn.commit()

try:
    cursor = conn.cursor()
    sql = "select avg(vl_total) from tb_customer_account\
           where vl_total > 560 and\
           id_customer between 1500 and 2700" #Isso é uma query para tirar a média do vl_total no qual vl_total é maior que 560 e id_customer está entre 1500 e 2700
    cursor.execute(sql)
    result = cursor.fetchone()
    print("A média do vl_total foi R$",result['vl_total'])
    cursor.close()
except:
    print("Aconteceu um erro inesperado...")
    cursor.close()

try:
    cursor = conn.cursor()
    sql = "select nm_customer, vl_total from tb_customer_account\
           where vl_total > 560 and\
           id_customer between 1500 and 2700 order by vl_total desc" #Isso é uma query para tirar a média do vl_total no qual vl_total é maior que 560 e id_customer está entre 1500 e 2700
    cursor.execute(sql)
    result = cursor.fetchall()
    
    for j in result:
        print("Nome: {}\n Valor total: {}\n\n".format(j['nm_customer'], j['vl_total']))
except:
    print("Algo inesperado aconteceu...")
    cursor.close()

conn.close()
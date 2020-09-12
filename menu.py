from flask import Flask,render_template,request,redirect,url_for
from Utils.db_connect import db_connect
import mysql.connector
app = Flask(__name__)                           #создаем новый экземпляр объекта типа фласк

@app.route('/menu',methods=['GET','POST'])      
def menu():
    try:
        point = request.args['point']
    except:
        point = None
        
    if (point == '2'):
        return redirect(url_for('zapros2'))
    if (point == '5'):
        return redirect(url_for('zapros5'))
    if (point == '7'):
        return redirect(url_for('zapros7'))
    elif (point =='exit'):
        return redirect(url_for('exit'))
    else:
        try:
            point = request.form['send']
        except:
            point=None
            
        if (point == '1'):
            return redirect(url_for('procedure'))
        if (point == '3'):
            return redirect(url_for('zapros3'))
        if (point == '4'):
            return redirect(url_for('zapros4'))
        if (point == '6'):
            return redirect(url_for('zapros6'))
        return render_template('menu.html')
 
@app.route('/1annual_report', methods=['GET','POST'])
def procedure():
    _year = request.form['grind']
    conn = db_connect()
    cursor = conn.cursor()
    #t_year = 2021
    q = check(_year)                    # Вызвали функцию проверки наличия таких отчетов в БД
    if (q == 0):                        # Таких отчетов нет в БД
        #args = (_year)
        result = cursor.callproc('annual_report',(_year,)) # result содержит входные параметры
        conn.commit()
        #print('result=',result)         #Отладочная печать
        mes='Отчет только что успешно создан'
    else:
        mes='Такой отчет был создан ранее'
        
    proc="""SELECT id_rep, id_structure, name_vac, num_open, avg_num_days
        FROM report
        WHERE _year = %s;"""
    cursor.execute(proc,(_year,))
    result = cursor.fetchall() 
    
    res = []
    schema = [ 'id_rep', 'id_structure', 'name_vac', 'num_open', 'avg_num_days']
    for data in result:
        res.append(dict(zip(schema, data)))
    return render_template('1annual_report.html', grind = _year,  results = res, mes=mes)

def check(_year):
    conn = db_connect()
    cursor = conn.cursor()
    ch = """SELECT COUNT(*) 
        FROM report
        WHERE _year = %s;"""
    cursor.execute(ch,(_year,))
    result = cursor.fetchall()
    q = result[0][0]
    return q
        
@app.route('/2open_vacansy',methods=['GET','POST']) 
def zapros2():
    #name_patient = request.form['grind']
    #print('data=', name_patient)
    conn = db_connect()         
    print('conn=',conn)
    cursor = conn.cursor()                  #позволяет осуществлять обмен данными с БД через открытое соединение
    z2 = """SELECT id_vac, unit_vac, DATEDIFF(SYSDATE(), date_open)
        FROM `recruiting`.`vacancy`
        WHERE date_close IS NULL;"""
    cursor.execute(z2)                      #передача SQL запроса базе данных
    result = cursor.fetchall()              #получение результата
        
    res = []
    schema = [ 'id_vac', 'unit_vac', 'amount_of_days'] 
    for data in result:
        res.append(dict(zip(schema,data)))
    return render_template('2open_vacansy.html', results = res)

@app.route('/3vac_movement',methods=['GET','POST'])    
def zapros3():
    _year = request.form['grind']
    conn = db_connect()
    cursor = conn.cursor()
    z3 = """SELECT id_struct, unit_vac, COUNT(*) AS number_of_open
        FROM `recruiting`.`vacancy`
        WHERE YEAR(date_open) = %s
        GROUP BY id_struct;"""
    cursor.execute(z3,(_year,))
    result = cursor.fetchall()
     
    res = []
    schema = [ 'id_struct', 'unit_vac', 'number_of_open']
    for data in result:
        res.append(dict(zip(schema, data)))
    return render_template('3vac_movement.html', grind = _year,  results = res)
        
@app.route('/4youngest',methods=['GET','POST'])    
def zapros4():
    unit = request.form['grind']
    conn = db_connect()
    cursor = conn.cursor()
    z4 = """SELECT `co-worker`.*
        FROM `recruiting`.`co-worker`
        JOIN staffing_table ON `co-worker`.id_struct = staffing_table.id_struct
        WHERE unit=%s AND date_dis IS NULL
        ORDER BY birth DESC
        LIMIT 1;"""
    cursor.execute(z4,(unit,))
    result = cursor.fetchall()
     
    res = []
    schema = [ 'id_worker', 'FName_worker', 'birth', 'address_w', 'educat', 'salary', 'date_res', 'date_dis', 'id_struct']
    for data in result:
        res.append(dict(zip(schema, data)))
    return render_template('4youngest.html', grind = unit,  results = res)

@app.route('/5vac_didnt_open',methods=['GET','POST']) 
def zapros5():
    #name_patient = request.form['grind']
    #print('data=', name_patient)
    conn = db_connect()         
    print('conn=',conn)
    cursor = conn.cursor()                  #позволяет осуществлять обмен данными с БД через открытое соединение
    z5 = """SELECT staffing_table.*
        FROM `recruiting`.`staffing_table`
        LEFT JOIN `recruiting`.`vacancy`
        ON staffing_table.id_struct = vacancy.id_struct
        WHERE vacancy.id_struct IS NULL;"""
    cursor.execute(z5)                      #передача SQL запроса базе данных
    result = cursor.fetchall()              #получение результата
        
    res = []
    schema = ['id_struct', 'unit', 'minmax_salary'] 
    for data in result:
        res.append(dict(zip(schema,data)))
    return render_template('5vac_not_open.html', results = res)
    
@app.route('/6vac_didnt_open_in_year',methods=['GET','POST'])    
def zapros6():
    _year = request.form['grind']
    conn = db_connect()
    cursor = conn.cursor()
    z6 = """SELECT staffing_table.*
        FROM `recruiting`.`staffing_table`
        LEFT JOIN (
            SELECT *
            FROM `recruiting`.`vacancy`
            WHERE YEAR(vacancy.date_open) = %s) AS year_vacancy
        ON staffing_table.id_struct = year_vacancy.id_struct
        WHERE year_vacancy.id_struct IS NULL; """
    cursor.execute(z6,(_year,))
    result = cursor.fetchall()
     
    res = []
    schema = ['id_struct', 'unit', 'minmax_salary']
    for data in result:
        res.append(dict(zip(schema, data)))
    return render_template('6vac_didnt_open_in_year.html', grind = _year,  results = res)    
    
@app.route('/7vac_most_often',methods=['GET','POST']) 
def zapros7():
    #name_patient = request.form['grind']
    #print('data=', name_patient)
    conn = db_connect()         
    print('conn=',conn)
    cursor = conn.cursor()                  #позволяет осуществлять обмен данными с БД через открытое соединение
    z7_drop = """DROP VIEW view_vacancy;"""    
    z7_create="""CREATE VIEW view_vacancy
        AS SELECT id_struct, unit_vac, COUNT(*)
        FROM `recruiting`.`vacancy`
        GROUP BY id_struct
        ORDER BY COUNT(*) DESC
        LIMIT 1;"""
    z7="""SELECT staffing_table.*
        FROM staffing_table
        INNER JOIN `recruiting`.`view_vacancy`
        ON staffing_table.id_struct = view_vacancy.id_struct;"""
    cursor.execute(z7_drop)
    cursor.execute(z7_create)
    cursor.execute(z7)                      #передача SQL запроса базе данных
    result = cursor.fetchall()              #получение результата
        
    res = []
    schema = ['id_struct', 'unit', 'minmax_salary'] 
    for data in result:
        res.append(dict(zip(schema,data)))
    return render_template('7vac_most_often.html', results = res)
    
@app.route('/exit',methods=['GET','POST']) 
def exit():
    return render_template('exit.html')
    
    
app.run(port=5001,debug=True)               # запуск веб сервера
    

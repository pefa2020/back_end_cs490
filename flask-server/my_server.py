from flask import Flask, request, jsonify
import mysql.connector
from flask_cors import CORS

# ***FLASK PART***
app = Flask(__name__)
CORS(app)

# Reasons why a film can't be rented out:
"""
    Decide to merge the 2 stores (so i don't have to worry about stores)
Simple difference:
RECEIVE: film_id
value = a(total copies of that film available in inventory) - b(total copies of that film rented out in rental)
A) keep track of inventory_id s
B) keep track of inventory_id s
if value < 1:
	we cannot rent it. A copy of the film is not available (all are rented out).
else:
	we can rent it. (using inventory id of the one that we can rent)
	Pick from pool of copies that one can rent.
	pool = all inventory ids from A that are not in B
	pick any inventory_id from pool and proceed rent (add row to rental)
    
"""
def checkIfCopyAvailable(film_id):
    # checks if a film can't be rented. Film = list of invetory ids
    cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
    cursor = cnx.cursor()
    cursor.execute("SELECT TA.inventory_id FROM (SELECT INVENTORY.inventory_id FROM INVENTORY WHERE INVENTORY.film_id = %s) AS TA WHERE NOT EXISTS (SELECT TB.inventory_id FROM (SELECT INVENTORY.inventory_id FROM INVENTORY JOIN RENTAL ON INVENTORY.inventory_id = RENTAL.inventory_id WHERE INVENTORY.film_id = %s AND RENTAL.return_date IS NULL) AS TB WHERE TA.inventory_id = TB.inventory_id);", (film_id, film_id))
    availableForRent = cursor.fetchall()
    print(availableForRent)
    return availableForRent


@app.route('/validateCustomer', methods=['POST'])
def validateCustomer():
    data = request.get_json()
    film_id = data[0]
    customer_id = int(data[1])
    print([film_id, customer_id])
    
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        cursor.execute("SELECT CUSTOMER.store_id FROM CUSTOMER WHERE CUSTOMER.customer_id = %s", (customer_id,))
        result = cursor.fetchall()
        print("result is: ", result, " with length: ", len(result))
        if (len(result) < 1):
            return jsonify("CUSTOMER_ID does NOT exist.") #jsonify({'message from validateCustomer': 'Data Received BUT CUSTOMER_ID does NOT exist}'}), 200
        store_id = result[0][0]
        available_rent = checkIfCopyAvailable(film_id)
        if (len(available_rent) == 0):
            return jsonify("The film you wanted is not available for rent right now.") # jsonify({'message': 'The film you wanted is not available for rent right now.'})
        
        # We know store_id = staff_id
        print("About to insert inventory_id: ", available_rent[0][0], " customer_id: ", customer_id, " staff_id: ", store_id)
        cursor.execute("INSERT INTO RENTAL(rental_date, inventory_id, customer_id, return_date, staff_id, last_update) VALUES(NOW(), %s, %s, NULL, %s, NOW())", (available_rent[0][0], customer_id, store_id))
        cnx.commit()
        return  jsonify("Data received and processed. ROW INSERTED.") # 1 means success: jsonify({'message from validateCustomer': 'Data received and processed. ROW INSERTED'})
    except Exception as e:
        return jsonify("Could not establish a connection with database.") # jsonify({'message from validateCustomer': 'Could not establish a connection with database'}), 200

@app.route('/genreField', methods=['POST'])
def filmByGenre():
    data = request.get_json()
    category_name = data[0].capitalize() + "%"
    print(category_name)
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        try:
            cursor.execute("SELECT FILM.film_id, FILM.title, FILM.description, FILM.release_year, FILM.rental_rate, FILM.rating, FILM.special_features FROM FILM JOIN FILM_CATEGORY ON FILM_CATEGORY.film_id = FILM.film_id JOIN CATEGORY ON CATEGORY.category_id = FILM_CATEGORY.category_id WHERE CATEGORY.name LIKE %s", (str(category_name),))
            row_headers = [x[0] for x in cursor.description]
            result = cursor.fetchall()
            json_data = []
            for i in result:
                my_dict = {}
                for k1, k2 in zip(row_headers, i):
                    if isinstance(k2, set):
                        k2 = ', '.join(k2)
                    my_dict[k1] = k2
                json_data.append(my_dict)
            print(json_data)
            return jsonify(json_data)
        except Exception as e:
            return jsonify({'message from filmByGenre': 'Data Received BUT QUERY does not work}'}), 200
        
    except Exception as e:
        return jsonify({'message from filmByGenre': 'Something went wrong'})

@app.route('/actorField', methods=['POST'])
def filmByActor():
    data = request.get_json()
    print("this is data: ", data)
    # CONSIDERING both first name and/or last name of actor
    actor_first_name = data[0].upper() + "%"
    actor_last_name = data[1].upper() + "%"
    print(actor_first_name, actor_last_name)
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        try:
            cursor.execute("SELECT DISTINCT(FILM.film_id), FILM.title, FILM.description, FILM.release_year, FILM.rental_rate, FILM.rating, FILM.special_features FROM FILM JOIN FILM_ACTOR ON FILM_ACTOR.film_id = FILM.film_id JOIN ACTOR ON FILM_ACTOR.actor_id = ACTOR.actor_id WHERE ACTOR.first_name LIKE %s AND ACTOR.last_name LIKE %s", (str(actor_first_name),str(actor_last_name)))
            row_headers = [x[0] for x in cursor.description]
            result = cursor.fetchall()
            json_data = []
            for i in result:
                my_dict = {}
                for k1, k2 in zip(row_headers, i):
                    if isinstance(k2, set):
                        k2 = ', '.join(k2)
                    my_dict[k1] = k2
                json_data.append(my_dict)
            print(json_data)
            return jsonify(json_data)
        except Exception as e:
            return jsonify({'message from filmByActor': 'Data Received BUT QUERY does not work}'}), 200

    except Exception as e:
        return jsonify({'message from filmByACtor': 'Something went wrong'})

@app.route('/filmField', methods=['POST'])
def filmByFilm():
    data = request.get_json()
    film_name = data[0].upper() + "%"
    print("-", film_name, "-")
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        try:
            cursor.execute("SELECT FILM.film_id, FILM.title, FILM.description, FILM.release_year, FILM.rental_rate, FILM.rating, FILM.special_features FROM FILM WHERE FILM.title LIKE %s", (str(film_name),))
            row_headers = [x[0] for x in cursor.description]
            result = cursor.fetchall()
            json_data = []
            for i in result:
                my_dict = {}
                for k1, k2 in zip(row_headers, i):
                    if isinstance(k2, set):
                        k2 = ', '.join(k2)
                    my_dict[k1] = k2
                json_data.append(my_dict)
            print(json_data)
            return jsonify(json_data)
        except Exception as e:
            return jsonify({'message from filmByFilm': 'Data Received BUT QUERY does not work}'}), 200
        
    except Exception as e:
        return jsonify({'message from filmByFilm': 'Something went wrong'})


@app.route("/actor/<int:actor_id>", methods=['GET'])
def actorTopMovies(actor_id):
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        cursor.execute("SELECT FILM_ACTOR.actor_id, FILM.title FROM FILM_ACTOR JOIN INVENTORY ON INVENTORY.film_id = FILM_ACTOR.film_id JOIN RENTAL ON RENTAL.inventory_id = INVENTORY.inventory_id JOIN FILM ON FILM.film_id = FILM_ACTOR.film_id WHERE FILM_ACTOR.actor_id = %s GROUP BY FILM_ACTOR.actor_id, FILM_ACTOR.film_id ORDER BY COUNT(RENTAL.inventory_id) DESC LIMIT 5", (actor_id,))
        row_headers = [x[0] for x in cursor.description] # added
        data = cursor.fetchall()
        print(data)
        json_data = []
        for i in data:
            my_dict = {}
            for k1, k2 in zip(row_headers, i):
                my_dict[k1] = k2
            json_data.append(my_dict)
        
        cursor.close()
        #cnx.close()
        # jsonify(data)
        return jsonify(json_data)
    except Exception as e:
        return f"{str(e)}"


@app.route("/topActors")
def topFiveActors():
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        query = 'SELECT ACTOR.actor_id, ACTOR.first_name, ACTOR.last_name FROM ACTOR JOIN FILM_ACTOR ON ACTOR.actor_id = FILM_ACTOR.actor_id GROUP BY ACTOR.actor_id ORDER BY COUNT(FILM_ACTOR.actor_id) DESC LIMIT 5'
        cursor.execute(query)
        row_headers = [x[0] for x in cursor.description] # added
        data = cursor.fetchall()
        json_data = []
        for result in data:
            json_data.append(dict(zip(row_headers, result)))
        cursor.close()
        #cnx.close()
        # jsonify(data)
        return jsonify(json_data)
    except Exception as e:
        return f"{str(e)}"


@app.route("/film/<int:film_id>", methods=['GET'])
def getFilmDetails(film_id):
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        cursor.execute("SELECT * FROM FILM WHERE film_id = %s", (film_id,))
        row_headers = [x[0] for x in cursor.description]
        result = cursor.fetchall()
        json_data = []
        for i in result:
            my_dict = {}
            for k1, k2 in zip(row_headers, i):
                if isinstance(k2, set):
                    k2 = ', '.join(k2)
                my_dict[k1] = k2
            json_data.append(my_dict)
        # print(json_data)
        return jsonify(json_data)
    except Exception as e:
        return f"{str(e)}"

@app.route("/top5")
def topFiveFilms():
    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='password',database='sakila')
        cursor = cnx.cursor()
        query = 'SELECT FILM.film_id, FILM.title FROM FILM JOIN INVENTORY ON INVENTORY.FILM_ID = FILM.film_id JOIN RENTAL ON INVENTORY.inventory_id = RENTAL.inventory_id GROUP BY FILM.film_id ORDER BY COUNT(INVENTORY.film_id) DESC LIMIT 5'
        cursor.execute(query)
        row_headers = [x[0] for x in cursor.description] # added
        data = cursor.fetchall()
        json_data = []
        for result in data:
            json_data.append(dict(zip(row_headers, result)))
        cursor.close()
        #cnx.close()
        return jsonify(json_data)
    except Exception as e:
        return f"{str(e)}"

if __name__ == "__main__":
    app.run(debug=True, port=5000, host="localhost")
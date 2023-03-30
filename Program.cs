using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Npgsql;
using System;
using System.Data;
using System.Globalization;

namespace DB1
{
    class Program
    {
        static void Main(string[] args)
        {
            
            //создание таблицы клиентов
            CreateClientsTable();

            //создание таблицы курсов
            CreateCourseTable();

            //создание таблицы депозитов
            CreateDepositsTable();

            //вставка пяти записей в таблицы
            for (int i = 1; i < 6; i++)
            {
                //вставка записи клиента
                string first_name = "Иван";
                string last_name = "Иванов";
                string middle_name = "Иванович";
                string email = "ivan" + i.ToString() + "@mail.ru";
                var clientId = InsertClientsWithParams(first_name, last_name, middle_name, email);

                //вставка записи курса
                string name = "C Sharp Developer: " + i.ToString() + " уровень";
                int price = 10000 * i;
                var courseId = InsertCourseWithParams(name, price);

                //вставка записи депозита
                InsertDepositWithParams(clientId, courseId, DateTime.Now);
            }
            

            //вывести таблицы в консоль
            ShowClients();
            ShowCourse();
            ShowDeposits();

            //добавление данных в таблицы на выбор
            InsertDataInTable();



            Console.ReadKey();



            const string connectionString = "Host=localhost;Username=postgres;Password=11061986;Database=Otus";


            /// <summary>
            /// Создание таблицы с данными о клиентах-учениках
            /// </summary>
            static void CreateClientsTable()
            {
                using var connection = new NpgsqlConnection(connectionString);

                connection.Open();

                var sql = @"
CREATE SEQUENCE clients_id_seq;

CREATE TABLE clients
(
    id              BIGINT                      NOT NULL    DEFAULT NEXTVAL('clients_id_seq'),
    first_name      CHARACTER VARYING(255)      NOT NULL,
    last_name       CHARACTER VARYING(255)      NOT NULL,
    middle_name     CHARACTER VARYING(255),
    email           CHARACTER VARYING(255)      NOT NULL,
  
    CONSTRAINT clients_pkey PRIMARY KEY (id),
    CONSTRAINT clients_email_unique UNIQUE (email)
);
";

                using var cmd = new NpgsqlCommand(sql, connection);

                var affectedRowsCount = cmd.ExecuteNonQuery().ToString();

                Console.WriteLine($"Created CLIENTS table. Affected rows count: {affectedRowsCount}");
            }


            /// <summary>
            /// Создание таблицы с курсами
            /// </summary>
            static void CreateCourseTable()
            {
                using var connection = new NpgsqlConnection(connectionString);

                connection.Open();

                var sql = @"
CREATE SEQUENCE course_id_seq;

CREATE TABLE course
(
    id              BIGINT                      NOT NULL    DEFAULT NEXTVAL('course_id_seq'),
    name            CHARACTER VARYING(255)      NOT NULL,
    price           NUMERIC                     NOT NULL,
 
    CONSTRAINT course_pkey PRIMARY KEY (id),
    CONSTRAINT unique_course_name UNIQUE (name)
);
";

                using var cmd = new NpgsqlCommand(sql, connection);

                var affectedRowsCount = cmd.ExecuteNonQuery().ToString();

                Console.WriteLine($"Created COURSE table. Affected rows count: {affectedRowsCount}");
            }

            /// <summary>
            /// Создание таблицы депозитов с внешним ключом
            /// </summary>
            static void CreateDepositsTable()
            {
                using var connection = new NpgsqlConnection(connectionString);

                connection.Open();

                var sql = @"
CREATE SEQUENCE deposits_id_seq;

CREATE TABLE deposits
(
    id              BIGINT                      NOT NULL    DEFAULT NEXTVAL('deposits_id_seq'),
    client_id       BIGINT                      NOT NULL,
    course_id       BIGINT                      NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE    NOT NULL,    
  
    CONSTRAINT deposits_pkey PRIMARY KEY (id),
    CONSTRAINT deposits_fk_client_id FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
    CONSTRAINT deposits_fk_course_id FOREIGN KEY (course_id) REFERENCES course(id) ON DELETE CASCADE
);
";

                using var cmd = new NpgsqlCommand(sql, connection);

                var affectedRowsCount = cmd.ExecuteNonQuery().ToString();

                Console.WriteLine($"Created DEPOSITS table. Affected rows count: {affectedRowsCount}");
            }

            /// <summary>
            /// Вставка клиента с параметрами
            /// </summary>
            static long InsertClientsWithParams(string first_name, string last_name, string middle_name, string email)
            {
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
INSERT INTO clients(first_name, last_name, middle_name, email) 
VALUES (:first_name, :last_name, :middle_name, :email)
RETURNING id;
";

                using var cmd = new NpgsqlCommand(sql, connection);
                var parameters = cmd.Parameters;
                parameters.Add(new NpgsqlParameter("first_name", first_name));
                parameters.Add(new NpgsqlParameter("last_name", last_name));
                parameters.Add(new NpgsqlParameter("middle_name", middle_name));
                parameters.Add(new NpgsqlParameter("email", email));

                var clientId = (long)cmd.ExecuteScalar();

                Console.WriteLine($"Insert into CLIENTS table. Affected rows count: {clientId}");

                return clientId;
            }

            /// <summary>
            /// Вставка курса с параметрами
            /// </summary>
            static long InsertCourseWithParams(string name, int price)
            {
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
INSERT INTO course(name, price) 
VALUES (:name, :price)
RETURNING id;
";

                using var cmd = new NpgsqlCommand(sql, connection);
                var parameters = cmd.Parameters;
                parameters.Add(new NpgsqlParameter("name", name));
                parameters.Add(new NpgsqlParameter("price", price));

                var courseId = (long)cmd.ExecuteScalar();

                Console.WriteLine($"Insert into COURSE table. Affected rows count: {courseId}");

                return courseId;
            }

            /// <summary>
            /// Вставка депозита с параметрами
            /// </summary>
            static void InsertDepositWithParams(long client_id, long course_id, DateTime created_at)
            {
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
INSERT INTO deposits(client_id, course_id, created_at) 
VALUES (:client_id, :course_id, :created_at);
";

                using var cmd = new NpgsqlCommand(sql, connection);
                var parameters = cmd.Parameters;
                parameters.Add(new NpgsqlParameter("client_id", client_id));
                parameters.Add(new NpgsqlParameter("course_id", course_id));
                parameters.Add(new NpgsqlParameter("created_at", created_at));

                var affectedRowsCount = cmd.ExecuteNonQuery().ToString();

                Console.WriteLine($"Insert into DEPOSITS table. Affected rows count: {affectedRowsCount}");
            }


            /// <summary>
            /// Отображение содержимого таблицы клиентов
            /// </summary>
            static void ShowClients()
            {
                Console.WriteLine("Таблица clients:");

                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
SELECT id, first_name, last_name, middle_name, email FROM clients;
";

                using var cmd1 = new NpgsqlCommand(sql, connection);
                {
                    var reader = cmd1.ExecuteReader();

                    while (reader.Read())
                    {
                        var id = reader.GetInt64(0);
                        var firstName = reader.GetString(1);
                        var lastName = reader.GetString(2);
                        var middleName = reader.GetString(3);
                        var email = reader.GetString(4);

                        Console.WriteLine($"clientid={id}, firstName={firstName},lastName={lastName},middleName={middleName},email={email}");
                    }
                }
            }

            /// <summary>
            /// Отображение содержимого  таблицы курсов
            /// </summary>
            static void ShowCourse()
            {
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
SELECT id, name, price FROM course;
";

                using var cmd2 = new NpgsqlCommand(sql, connection);
                var reader = cmd2.ExecuteReader();

                Console.WriteLine("Таблица course:");

                while (reader.Read())
                {
                    var id = reader.GetInt64(0);
                    var name = reader.GetString(1);
                    var price = reader.GetInt64(2);

                    Console.WriteLine($"courseid={id}, Name={name},Price={price}");
                }
            }


            /// <summary>
            /// Отображение содержимого таблицы депозитов
            /// </summary>
            static void ShowDeposits()
            {
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
SELECT client_id, course_id, created_at FROM deposits;
";

                using var cmd3 = new NpgsqlCommand(sql, connection);
                var reader = cmd3.ExecuteReader();

                Console.WriteLine("Таблица deposits:");

                while (reader.Read())
                {
                    var clientid = reader.GetInt64(0);
                    var courseid = reader.GetInt64(1);
                    var createdat = reader.GetDateTime(2);

                    Console.WriteLine($"ClientID={clientid},CourseID={courseid}, CreatedAt={createdat}");
                }
            }

            /// <summary>
            /// Вставка данных в таблицу на выбор
            /// </summary>
            static void InsertDataInTable()
            {
                int table_number;

                do
                {
                    Console.WriteLine("Выберите в какую таблицу добавлять: 1 - clients, 2 - course, 3 - deposits");

                    while (!int.TryParse(Console.ReadLine(), out table_number))
                        Console.WriteLine("Выберите в какую таблицу добавлять: 1 - clients, 2 - course, 3 - deposits");
                }
                while (table_number < 1 || table_number > 3);

                //если будем добавлять данные в 1 - clients
                if (table_number == 1)
                {
                    string[] client_data;

                    do
                    {
                        Console.WriteLine("Введите данные в формате: firstname,lastname,middlename,email");
                        client_data = Console.ReadLine().Split(new char[] { ',' });
                    }
                    while (!(client_data.Length == 4 && !EmailExist(client_data[3])));

                    InsertClientsWithParams(client_data[0], client_data[1], client_data[2], client_data[3]);
                }
                //если будем добавлять данные в 2 - course
                else if (table_number == 2)
                {
                    string[] course_data;
                    int course_price;

                    do
                    {
                        Console.WriteLine("Введите данные в формате: name,price");
                        course_data = Console.ReadLine().Split(new char[] { ',' });
                    }
                    while (!(course_data.Length == 2 && int.TryParse(course_data[1], out course_price) && !CourseExist(course_data[0])));

                    InsertCourseWithParams(course_data[0], course_price);
                }
                //если будем добавлять данные в 3 - deposits
                else
                {
                    string[] deposit_data;
                    long clientid, courseid;
                    DateTime dateTime;

                    do
                    {
                        Console.WriteLine("Введите данные в формате: clientid,courseid,dd.MM.yyyy HH:mm");
                        deposit_data = Console.ReadLine().Split(new char[] { ',' });
                    }
                    while (!(deposit_data.Length == 3 && long.TryParse(deposit_data[0], out clientid) &&
                    long.TryParse(deposit_data[1], out courseid) && DateTime.TryParseExact(deposit_data[2], "dd.MM.yyyy HH:mm", null, DateTimeStyles.None, out dateTime)
                    && ClientIDExist(clientid) && CourseIDExist(courseid)));

                    InsertDepositWithParams(clientid, courseid, dateTime);
                }
            }

            /// <summary>
            /// Проверка существования email в таблице клиентов
            /// </summary>
            static bool EmailExist(string email)
            {
                bool output = false;
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
SELECT id FROM clients WHERE email =:email
";

                using var cmd1 = new NpgsqlCommand(sql, connection);
                var parameters = cmd1.Parameters;
                parameters.Add(new NpgsqlParameter("email", email));

                var reader = cmd1.ExecuteReader();
                while (reader.Read())
                {
                    var id = reader.GetInt64(0);
                    Console.WriteLine($"Найден клиент с таким email, его Id = {id}");
                    output = true;
                }

                return output;
            }

            /// <summary>
            /// Проверка существования id в таблице клиентов
            /// </summary>
            static bool ClientIDExist(long id)
            {
                bool output = false;
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
SELECT id FROM clients WHERE id =:id
";

                using var cmd1 = new NpgsqlCommand(sql, connection);
                var parameters = cmd1.Parameters;
                parameters.Add(new NpgsqlParameter("id", id));

                var reader = cmd1.ExecuteReader();
                if (reader.Read())
                {
                    var find_id = reader.GetInt64(0);
                    Console.WriteLine($"Найден клиент с таким Id = {find_id}.");
                    output = true;
                }
                else
                    Console.WriteLine($"Клиент с Id = {id} не найден.");

                return output;
            }


            /// <summary>
            /// Проверка существования названия курса в таблице курсов
            /// </summary>
            static bool CourseExist(string name)
            {
                bool output = false;
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
SELECT id FROM course WHERE name =:name
";

                using var cmd1 = new NpgsqlCommand(sql, connection);
                var parameters = cmd1.Parameters;
                parameters.Add(new NpgsqlParameter("name", name));

                var reader = cmd1.ExecuteReader();
                if (reader.Read())
                {
                    var id = reader.GetInt64(0);
                    Console.WriteLine($"Найден курс с таким же названием, его Id = {id}.");
                    output = true;
                }
                else
                    Console.WriteLine($"Курс с названием {name} не найден.");

                return output;
            }

            /// <summary>
            /// Проверка существования id курса в таблице курсов
            /// </summary>
            static bool CourseIDExist(long id)
            {
                bool output = false;
                using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var sql = @"
SELECT id FROM course WHERE id =:id
";

                using var cmd1 = new NpgsqlCommand(sql, connection);
                var parameters = cmd1.Parameters;
                parameters.Add(new NpgsqlParameter("id", id));

                var reader = cmd1.ExecuteReader();
                if (reader.Read())
                {
                    var find_id = reader.GetInt64(0);
                    Console.WriteLine($"Найден курс с таким Id = {id}.");
                    output = true;
                }
                else
                    Console.WriteLine($"Курс с таким Id = {id} не найден.");

                return output;
            }
        }
    }
}


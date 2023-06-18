from jsondb import JsonDb


def main():
    db_obj = JsonDb("test.json", add_id=True)
    db_obj.insert({"name": "John", "age": 20})
    db_obj.insert({"name": "Jane", "age": 21})
    db_obj.insert({"name": "Jonathan", "age": 21})

    db_obj = db_obj.select(["name", "age"]).where("eq", "name", "John")
    val = db_obj.fetch_value()
    print(val)

    db_obj = db_obj.where("all").update("name", "John")

    db_obj = db_obj.select(["name", "age"]).where("eq", "name", "John")
    val = db_obj.fetch_value()
    print(val)

    db_obj = db_obj.select()
    val = db_obj.fetch_value()
    print(val)

    # db_obj = db_obj.where("eq", "age", 21).delete()

    # db_obj = db_obj.select()
    # val = db_obj.fetch_value()
    # print(val)

    db_obj.commit()


if __name__ == "__main__":
    main()

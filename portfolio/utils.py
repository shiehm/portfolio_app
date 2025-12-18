def verify_username(username):
    length = len(username) > 1
    is_alphanum = username.isalnum()
    unique = username not in g.storage.all_users()
    return length and is_alphanum and unique

def verify_password(password):
    numbers = list(range(10))
    length = len(password) >= 8
    has_num = any(str(number) in password for number in numbers)
    return length and has_num


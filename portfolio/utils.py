def verify_username(username, existing):
    length = len(username) > 1
    is_alphanum = username.isalnum()
    unique = username not in existing
    return length and is_alphanum and unique

def verify_password(password):
    numbers = list(range(10))
    length = len(password) >= 8
    has_num = any(str(number) in password for number in numbers)
    return length and has_num
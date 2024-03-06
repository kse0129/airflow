def get_sftp():
    print("get SFTP ...")

def regist(name, sex, *args, **kwargs):
    print(f"이름: {name}")
    print(f"성별: {sex}")
    print(f"기타: {args}")
    email = kwargs.get("email") or None
    phone = kwargs.get("phone") or None
    if email:
        print(f"이메일: {email}")
    if phone:
        print(f"전화번호: {phone}")

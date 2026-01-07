import shlex


def parse_where(args: list) -> dict:
    '''
    Функция для извлучения из where
    
    Переменная args: значения из where
    '''
    if 'where' not in args:
        return {}
    
    try:
        idx = args.index('where')
        col = args[idx + 1]
        val = args[idx + 3]
        
        val = val.strip('"').strip("'")
        return {col: val}
    except IndexError:
        return {}

def parse_insert_values(user_input: str) -> list:
    '''
    Функция для извлечения из insert into
    
    Переменная user_input: Ввод пользователя
    '''
    start = user_input.find('(')
    end = user_input.rfind(')')
    
    if start == -1 or end == -1:
        return []
    
    content = user_input[start+1:end]

    lexer = shlex.shlex(content, posix=True)
    lexer.whitespace += ',' 
    lexer.wordchars += '.'
    return list(lexer)
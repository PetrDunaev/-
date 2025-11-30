"""
Программа для получения данных с маркетплейса Wildberries через API
и создания интерактивного дашборда
"""


import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Optional
import os
import sqlite3
import hashlib
import secrets
from contextlib import contextmanager


# Константы
POPULAR_CATEGORIES = [
    "Все категории",
    "Одежда",
    "Обувь",
    "Электроника",
    "Бытовая техника",
    "Красота и здоровье",
    "Дом и дача",
    "Спорт и отдых",
    "Детские товары",
    "Автотовары",
    "Книги",
    "Игрушки"
]

CATEGORY_FILTER_TYPES = ["Предустановленные категории", "Из загруженных данных"]

SESSION_KEYS = {
    'authenticated': 'authenticated',
    'username': 'username',
    'user_id': 'user_id',
    'selected_api_key_id': 'selected_api_key_id',
    'selected_product_category': 'selected_product_category',
    'selected_product_subcategory': 'selected_product_subcategory',
    'selected_product_sub_subcategory': 'selected_product_sub_subcategory',
    'category_filter_type': 'category_filter_type'
}

DEFAULT_DATE_RANGE_DAYS = 30


class UserManager:
    """Класс для управления пользователями и API ключами"""
    
    DB_PATH = "wb_users.db"
    
    def __init__(self):
        """Инициализация и создание таблиц в базе данных"""
        self._init_database()
    
    @contextmanager
    def _get_connection(self):
        """Контекстный менеджер для работы с базой данных"""
        conn = sqlite3.connect(self.DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_database(self):
        """Создание таблиц в базе данных"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Таблица пользователей
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    salt TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Таблица API ключей
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    api_key TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    UNIQUE(user_id, name)
                )
            """)
    
    def _hash_password(self, password: str, salt: str = None) -> tuple:
        """
        Хэширование пароля
        
        Args:
            password: Пароль для хэширования
            salt: Соль (если None, генерируется новая)
            
        Returns:
            Кортеж (хэш, соль)
        """
        if salt is None:
            salt = secrets.token_hex(16)
        password_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return password_hash.hex(), salt
    
    def register_user(self, username: str, password: str) -> bool:
        """
        Регистрация нового пользователя
        
        Args:
            username: Имя пользователя
            password: Пароль
            
        Returns:
            True если регистрация успешна, False если пользователь уже существует
        """
        try:
            password_hash, salt = self._hash_password(password)
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO users (username, password_hash, salt) VALUES (?, ?, ?)",
                    (username, password_hash, salt)
                )
            return True
        except sqlite3.IntegrityError:
            return False
    
    def authenticate_user(self, username: str, password: str) -> bool:
        """
        Аутентификация пользователя
        
        Args:
            username: Имя пользователя
            password: Пароль
            
        Returns:
            True если аутентификация успешна, False в противном случае
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT password_hash, salt FROM users WHERE username = ?",
                (username,)
            )
            result = cursor.fetchone()
            if result:
                stored_hash, salt = result
                password_hash, _ = self._hash_password(password, salt)
                return password_hash == stored_hash
        return False
    
    def get_user_id(self, username: str) -> Optional[int]:
        """
        Получение ID пользователя по имени
        
        Args:
            username: Имя пользователя
            
        Returns:
            ID пользователя или None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
            result = cursor.fetchone()
            return result[0] if result else None
    
    def add_api_key(self, user_id: int, name: str, api_key: str) -> bool:
        """
        Добавление API ключа для пользователя
        
        Args:
            user_id: ID пользователя
            name: Наименование API ключа
            api_key: API ключ
            
        Returns:
            True если добавление успешно, False если ключ с таким именем уже существует
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO api_keys (user_id, name, api_key) VALUES (?, ?, ?)",
                    (user_id, name, api_key)
                )
            return True
        except sqlite3.IntegrityError:
            return False
    
    def get_api_keys(self, user_id: int) -> List[Dict]:
        """
        Получение списка API ключей пользователя
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Список словарей с информацией о API ключах
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, name, api_key, is_active FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def get_api_key_by_id(self, user_id: int, key_id: int) -> Optional[str]:
        """
        Получение API ключа по ID
        
        Args:
            user_id: ID пользователя
            key_id: ID API ключа
            
        Returns:
            API ключ или None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT api_key FROM api_keys WHERE id = ? AND user_id = ?",
                (key_id, user_id)
            )
            result = cursor.fetchone()
            return result[0] if result else None
    
    def delete_api_key(self, user_id: int, key_id: int) -> bool:
        """
        Удаление API ключа
        
        Args:
            user_id: ID пользователя
            key_id: ID API ключа
            
        Returns:
            True если удаление успешно
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM api_keys WHERE id = ? AND user_id = ?",
                (key_id, user_id)
            )
            return cursor.rowcount > 0
    
    def update_api_key_name(self, user_id: int, key_id: int, new_name: str) -> bool:
        """
        Обновление наименования API ключа
        
        Args:
            user_id: ID пользователя
            key_id: ID API ключа
            new_name: Новое наименование
            
        Returns:
            True если обновление успешно
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE api_keys SET name = ? WHERE id = ? AND user_id = ?",
                    (new_name, key_id, user_id)
                )
            return True
        except sqlite3.IntegrityError:
            return False


class WildberriesAPI:
    """Класс для работы с API Wildberries"""
    
    BASE_URL = "https://statistics-api.wildberries.ru"
    
    def __init__(self, api_key: str):
        """
        Инициализация API клиента
        
        Args:
            api_key: API ключ от Wildberries
        """
        self.api_key = api_key
        self.headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Выполнение запроса к API
        
        Args:
            endpoint: URL эндпоинта
            params: Параметры запроса
            
        Returns:
            Ответ от API в формате JSON
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        # Формирование полного URL с параметрами для отображения
        full_url = url
        if params:
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{url}?{query_string}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            json_data = response.json()
            
            # Отображение запроса и ответа
            with st.expander(f"🔍 Запрос: {endpoint}", expanded=False):
                st.subheader("📤 Запрос")
                st.code(f"URL: {full_url}\nMethod: GET\nHeaders: {self.headers}", language="text")
                
                st.subheader("📥 Ответ (сырой JSON)")
                st.json(json_data)
            
            return json_data
        except requests.exceptions.RequestException as e:
            st.error(f"Ошибка при запросе к API: {e}")
            # Отображение запроса даже при ошибке
            with st.expander(f"🔍 Запрос: {endpoint} (ОШИБКА)", expanded=False):
                st.subheader("📤 Запрос")
                st.code(f"URL: {full_url}\nMethod: GET\nHeaders: {self.headers}", language="text")
                st.error(f"Ошибка: {e}")
            return {}
    
    def get_supplies(self, date_from: Optional[str] = None) -> pd.DataFrame:
        """
        Получение списка поставок
        
        Args:
            date_from: Дата начала в формате YYYY-MM-DD
            
        Returns:
            DataFrame с данными о поставках
        """
        params = {}
        if date_from:
            params["dateFrom"] = date_from
        
        data = self._make_request("/api/v1/supplier/supplies", params)
        
        if data:
            df = pd.DataFrame(data)
            if not df.empty:
                # Преобразование дат
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
            return df
        return pd.DataFrame()
    
    def get_orders(self, date_from: str, date_to: Optional[str] = None) -> pd.DataFrame:
        """
        Получение данных о заказах
        
        Args:
            date_from: Дата начала в формате YYYY-MM-DD
            date_to: Дата окончания в формате YYYY-MM-DD (опционально)
            
        Returns:
            DataFrame с данными о заказах
        """
        params = {"dateFrom": date_from}
        if date_to:
            params["dateTo"] = date_to
        
        data = self._make_request("/api/v1/supplier/orders", params)
        
        if data:
            df = pd.DataFrame(data)
            if not df.empty:
                # Преобразование дат
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                if 'lastChangeDate' in df.columns:
                    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])
            return df
        return pd.DataFrame()
    
    def get_sales(self, date_from: str, date_to: Optional[str] = None) -> pd.DataFrame:
        """
        Получение данных о продажах
        
        Args:
            date_from: Дата начала в формате YYYY-MM-DD
            date_to: Дата окончания в формате YYYY-MM-DD (опционально)
            
        Returns:
            DataFrame с данными о продажах
        """
        params = {"dateFrom": date_from}
        if date_to:
            params["dateTo"] = date_to
        
        data = self._make_request("/api/v1/supplier/sales", params)
        
        if data:
            df = pd.DataFrame(data)
            if not df.empty:
                # Преобразование дат
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                if 'lastChangeDate' in df.columns:
                    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])
            return df
        return pd.DataFrame()
    
    def get_stocks(self, date_from: Optional[str] = None) -> pd.DataFrame:
        """
        Получение данных об остатках товаров
        
        Args:
            date_from: Дата начала в формате YYYY-MM-DD
            
        Returns:
            DataFrame с данными об остатках
        """
        params = {}
        if date_from:
            params["dateFrom"] = date_from
        
        data = self._make_request("/api/v1/supplier/stocks", params)
        
        if data:
            df = pd.DataFrame(data)
            if not df.empty:
                # Преобразование дат
                if 'lastChangeDate' in df.columns:
                    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])
            return df
        return pd.DataFrame()
    
    def get_report_detail_by_period(self, date_from: str, date_to: str, 
                                     rrdid: Optional[int] = None) -> pd.DataFrame:
        """
        Получение детального отчета за период
        
        Args:
            date_from: Дата начала в формате YYYY-MM-DD
            date_to: Дата окончания в формате YYYY-MM-DD
            rrdid: ID отчета (опционально)
            
        Returns:
            DataFrame с детальным отчетом
        """
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        if rrdid:
            params["rrdid"] = rrdid
        
        data = self._make_request("/api/v1/supplier/reportDetailByPeriod", params)
        
        if data:
            df = pd.DataFrame(data)
            if not df.empty:
                # Преобразование дат
                date_columns = ['date', 'create_dt', 'order_dt', 'sale_dt', 'rr_dt']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col])
            return df
        return pd.DataFrame()
    
    def _get_commission_tariffs(self) -> Dict:
        """
        Получение данных о комиссиях из API тарифов
        
        Returns:
            Словарь с данными о комиссиях
        """
        url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            json_data = response.json()
            
            # Отображение запроса и ответа
            with st.expander("🔍 Запрос: /api/v1/tariffs/commission", expanded=False):
                st.subheader("📤 Запрос")
                st.code(f"URL: {url}\nMethod: GET\nHeaders: {self.headers}", language="text")
                
                st.subheader("📥 Ответ (сырой JSON)")
                st.json(json_data)
            
            return json_data
        except requests.exceptions.RequestException as e:
            st.error(f"Ошибка при запросе к API комиссий: {e}")
            # Отображение запроса даже при ошибке
            with st.expander("🔍 Запрос: /api/v1/tariffs/commission (ОШИБКА)", expanded=False):
                st.subheader("📤 Запрос")
                st.code(f"URL: {url}\nMethod: GET\nHeaders: {self.headers}", language="text")
                st.error(f"Ошибка: {e}")
            return {}
    
    def get_commissions_by_category_today(self) -> pd.DataFrame:
        """
        Получение данных о комиссиях по категориям на сегодняшний день
        
        Returns:
            DataFrame с данными о комиссиях по категориям
        """
        data = self._get_commission_tariffs()
        
        if not data:
            return pd.DataFrame()
        
        # Функция для рекурсивного поиска списка в структуре данных
        def find_list_in_data(obj, max_depth=5):
            """Рекурсивно ищет список в структуре данных"""
            if max_depth <= 0:
                return None
            if isinstance(obj, list) and len(obj) > 0:
                return obj
            if isinstance(obj, dict):
                # Проверяем все ключи словаря
                for key in ['data', 'commissions', 'items', 'results', 'list', 'values']:
                    if key in obj:
                        result = find_list_in_data(obj[key], max_depth - 1)
                        if result is not None:
                            return result
                # Если не нашли в стандартных ключах, проверяем все значения
                for value in obj.values():
                    result = find_list_in_data(value, max_depth - 1)
                    if result is not None:
                        return result
            return None
        
        # Преобразование данных в DataFrame
        # Структура ответа может быть разной, обрабатываем наиболее вероятные варианты
        df = pd.DataFrame()
        
        if isinstance(data, list):
            # Если это уже список, используем его
            if len(data) > 0 and isinstance(data[0], dict):
                df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Ищем список в словаре
            found_list = find_list_in_data(data)
            if found_list:
                df = pd.DataFrame(found_list)
            else:
                # Если список не найден, пытаемся преобразовать словарь напрямую
                # Проверяем, есть ли в словаре поля, которые могут быть данными
                if any(key in data for key in ['subject', 'category', 'commission', 'percent']):
                    df = pd.DataFrame([data])
        
        if df.empty:
            return pd.DataFrame()
        
        # Поиск поля с категорией (расширенный список возможных полей)
        category_col = None
        for field in ['subject', 'category', 'subject_name', 'category_name', 'name', 
                     'categoryId', 'subjectId', 'subject_id', 'category_id', 
                     'subjectName', 'categoryName', 'title', 'label']:
            if field in df.columns:
                category_col = field
                break
        
        if not category_col:
            # Если не нашли категорию, пытаемся использовать индекс
            return pd.DataFrame()
        
        # Поиск поля с комиссией (расширенный список возможных полей)
        commission_col = None
        for field in ['commission', 'commissionPercent', 'percent', 'value', 'rate',
                     'commission_percent', 'commission_percentage', 'percentage',
                     'commissionValue', 'commission_value', 'tariff', 'fee']:
            if field in df.columns:
                commission_col = field
                break
        
        if not commission_col:
            return pd.DataFrame()
        
        # Формирование результата
        result = df[[category_col, commission_col]].copy()
        
        # Переименование колонок
        result.columns = ['Категория', 'Процент комиссии']
        
        # Преобразование процента комиссии в числовой формат, если нужно
        if result['Процент комиссии'].dtype == 'object':
            # Пытаемся преобразовать строки в числа
            result['Процент комиссии'] = pd.to_numeric(
                result['Процент комиссии'].astype(str).str.replace(',', '.').str.replace('%', ''), 
                errors='coerce'
            )
        
        # Если есть дополнительные поля, добавляем их
        if 'quantity' in df.columns:
            result['Количество'] = df['quantity']
        elif 'count' in df.columns:
            result['Количество'] = df['count']
        
        return result
    
    def _get_logistics_tariffs(self, date: Optional[str] = None) -> Dict:
        """
        Получение данных о логистике из API тарифов
        
        Args:
            date: Дата в формате YYYY-MM-DD (опционально)
        
        Returns:
            Словарь с данными о логистике
        """
        url = "https://common-api.wildberries.ru/api/v1/tariffs/box"
        params = {}
        if date:
            params["date"] = date
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            json_data = response.json()
            
            # Формирование полного URL с параметрами для отображения
            full_url = url
            if params:
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                full_url = f"{url}?{query_string}"
            
            # Отображение запроса и ответа
            with st.expander("🔍 Запрос: /api/v1/tariffs/box", expanded=False):
                st.subheader("📤 Запрос")
                st.code(f"URL: {full_url}\nMethod: GET\nHeaders: {self.headers}", language="text")
                
                st.subheader("📥 Ответ (сырой JSON)")
                st.json(json_data)
            
            return json_data
        except requests.exceptions.RequestException as e:
            st.error(f"Ошибка при запросе к API логистики: {e}")
            # Отображение запроса даже при ошибке
            full_url = url
            if params:
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                full_url = f"{url}?{query_string}"
            with st.expander("🔍 Запрос: /api/v1/tariffs/box (ОШИБКА)", expanded=False):
                st.subheader("📤 Запрос")
                st.code(f"URL: {full_url}\nMethod: GET\nHeaders: {self.headers}", language="text")
                st.error(f"Ошибка: {e}")
            return {}
    
    def get_logistics_by_warehouse_today(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        Получение данных о логистике по складам за указанную дату за 1 единицу товара
        
        Args:
            date: Дата в формате YYYY-MM-DD (опционально, по умолчанию сегодня)
        
        Returns:
            DataFrame с данными о логистике по складам
        """
        data = self._get_logistics_tariffs(date)
        
        if not data:
            return pd.DataFrame()
        
        # Функция для рекурсивного поиска списка в структуре данных
        def find_list_in_data(obj, max_depth=5):
            """Рекурсивно ищет список в структуре данных"""
            if max_depth <= 0:
                return None
            if isinstance(obj, list) and len(obj) > 0:
                return obj
            if isinstance(obj, dict):
                # Проверяем все ключи словаря
                for key in ['data', 'tariffs', 'box', 'items', 'results', 'list', 'values', 'warehouses']:
                    if key in obj:
                        result = find_list_in_data(obj[key], max_depth - 1)
                        if result is not None:
                            return result
                # Если не нашли в стандартных ключах, проверяем все значения
                for value in obj.values():
                    result = find_list_in_data(value, max_depth - 1)
                    if result is not None:
                        return result
            return None
        
        # Преобразование данных в DataFrame
        # Структура ответа может быть разной, обрабатываем наиболее вероятные варианты
        df = pd.DataFrame()
        
        if isinstance(data, list):
            # Если это уже список, используем его
            if len(data) > 0 and isinstance(data[0], dict):
                df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Ищем список в словаре
            found_list = find_list_in_data(data)
            if found_list:
                df = pd.DataFrame(found_list)
            else:
                # Если список не найден, пытаемся преобразовать словарь напрямую
                # Проверяем, есть ли в словаре поля, которые могут быть данными
                if any(key in data for key in ['warehouse', 'warehouse_name', 'price', 'cost', 'tariff']):
                    df = pd.DataFrame([data])
        
        if df.empty:
            return pd.DataFrame()
        
        # Поиск поля со складом (расширенный список возможных полей)
        warehouse_col = None
        for field in ['warehouse_name', 'warehouse', 'warehouse_id', 'warehouseName', 
                     'warehouseId', 'name', 'title', 'label']:
            if field in df.columns:
                warehouse_col = field
                break
        
        # Поиск поля с логистикой (расширенный список возможных полей)
        logistics_col = None
        for field in ['price', 'cost', 'tariff', 'logistics', 'delivery_cost', 
                     'box_price', 'boxPrice', 'deliveryCost', 'value', 
                     'amount', 'fee', 'charge']:
            if field in df.columns:
                logistics_col = field
                break
        
        if not logistics_col:
            return pd.DataFrame()
        
        # Формирование результата
        if warehouse_col:
            result = df[[warehouse_col, logistics_col]].copy()
            result.columns = ['Склад', 'Логистика (₽)']
        else:
            result = df[[logistics_col]].copy()
            result.columns = ['Логистика (₽)']
            # Если нет информации о складе, добавляем общую строку
            result.insert(0, 'Склад', 'Общий')
        
        # Преобразование стоимости логистики в числовой формат, если нужно
        if result['Логистика (₽)'].dtype == 'object':
            # Пытаемся преобразовать строки в числа
            result['Логистика (₽)'] = pd.to_numeric(
                result['Логистика (₽)'].astype(str).str.replace(',', '.').str.replace('₽', '').str.strip(), 
                errors='coerce'
            )
        
        return result


def process_orders_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обработка и структурирование данных о заказах
    
    Args:
        df: DataFrame с данными о заказах
        
    Returns:
        Обработанный DataFrame
    """
    if df.empty:
        return df
    
    # Группировка по датам
    if 'date' in df.columns:
        df['date_only'] = df['date'].dt.date
        daily_orders = df.groupby('date_only').agg({
            'gNumber': 'count',
            'totalPrice': 'sum' if 'totalPrice' in df.columns else lambda x: 0
        }).reset_index()
        daily_orders.columns = ['Дата', 'Количество заказов', 'Сумма заказов']
        return daily_orders
    
    return df


def process_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обработка и структурирование данных о продажах
    
    Args:
        df: DataFrame с данными о продажах
        
    Returns:
        Обработанный DataFrame
    """
    if df.empty:
        return df
    
    # Группировка по датам
    if 'date' in df.columns:
        df['date_only'] = df['date'].dt.date
        daily_sales = df.groupby('date_only').agg({
            'gNumber': 'count',
            'totalPrice': 'sum' if 'totalPrice' in df.columns else lambda x: 0
        }).reset_index()
        daily_sales.columns = ['Дата', 'Количество продаж', 'Сумма продаж']
        return daily_sales
    
    return df


def get_category_column(df: pd.DataFrame) -> Optional[str]:
    """
    Поиск колонки с категорией товара в DataFrame
    
    Args:
        df: DataFrame для поиска
        
    Returns:
        Название колонки с категорией или None
    """
    if df.empty:
        return None
    
    # Список возможных названий колонок с категорией
    category_fields = [
        'subject', 'subjectName', 'subject_name',
        'category', 'categoryName', 'category_name',
        'categoryId', 'subjectId', 'subject_id', 'category_id',
        'subjectName', 'categoryName', 'title', 'label'
    ]
    
    for field in category_fields:
        if field in df.columns:
            return field
    
    return None


def filter_by_category(df: pd.DataFrame, category: Optional[str] = None) -> pd.DataFrame:
    """
    Фильтрация DataFrame по категории товара
    
    Args:
        df: DataFrame для фильтрации
        category: Название категории для фильтрации (None - без фильтрации)
        
    Returns:
        Отфильтрованный DataFrame
    """
    if df.empty or category is None:
        return df
    
    category_col = get_category_column(df)
    if category_col is None:
        return df
    
    # Фильтрация по категории (без учета регистра)
    filtered_df = df[df[category_col].astype(str).str.contains(category, case=False, na=False)].copy()
    return filtered_df


def get_available_categories(df: pd.DataFrame) -> List[str]:
    """
    Получение списка доступных категорий из DataFrame
    
    Args:
        df: DataFrame для извлечения категорий
        
    Returns:
        Список уникальных категорий
    """
    if df.empty:
        return []
    
    category_col = get_category_column(df)
    if category_col is None:
        return []
    
    categories = df[category_col].dropna().unique().tolist()
    return sorted([str(cat) for cat in categories if str(cat).strip()])


def parse_category_hierarchy(category_str: str) -> List[str]:
    """
    Парсинг иерархии категории из строки
    
    Args:
        category_str: Строка с категорией (например, "одежда-белье-боди")
        
    Returns:
        Список уровней категории (например, ["одежда", "белье", "боди"])
    """
    if not category_str:
        return []
    
    # Разделение по дефису, слешу или другим разделителям
    separators = ['-', '/', '>', '|']
    for sep in separators:
        if sep in str(category_str):
            return [part.strip() for part in str(category_str).split(sep) if part.strip()]
    
    # Если разделителей нет, возвращаем всю строку как один уровень
    return [str(category_str).strip()]


def get_category_levels(df: pd.DataFrame, level: int = 0) -> List[str]:
    """
    Получение списка категорий на определенном уровне иерархии
    
    Args:
        df: DataFrame для извлечения категорий
        level: Уровень иерархии (0 - первый уровень, 1 - второй и т.д.)
        
    Returns:
        Список уникальных категорий на указанном уровне
    """
    if df.empty:
        return []
    
    category_col = get_category_column(df)
    if category_col is None:
        return []
    
    categories = df[category_col].dropna().unique().tolist()
    level_categories = set()
    
    for cat in categories:
        hierarchy = parse_category_hierarchy(str(cat))
        if len(hierarchy) > level:
            level_categories.add(hierarchy[level])
    
    return sorted(list(level_categories))


def _get_filter_text(category: str, subcategory: Optional[str] = None, 
                     sub_subcategory: Optional[str] = None) -> str:
    """
    Формирование текста фильтра для отображения
    
    Args:
        category: Категория
        subcategory: Подкатегория
        sub_subcategory: Под-подкатегория
        
    Returns:
        Текст фильтра
    """
    filter_text = category
    if subcategory:
        filter_text += f" → {subcategory}"
    if sub_subcategory:
        filter_text += f" → {sub_subcategory}"
    return filter_text


def _get_category_filters(category_filter_type: str, selected_category: Optional[str],
                         session_state: Dict) -> tuple:
    """
    Получение параметров фильтрации категорий
    
    Args:
        category_filter_type: Тип фильтра категории
        selected_category: Выбранная категория
        session_state: Состояние сессии
        
    Returns:
        Кортеж (category_to_filter, subcategory_to_filter, sub_subcategory_to_filter)
    """
    category_to_filter = None
    subcategory_to_filter = None
    sub_subcategory_to_filter = None
    
    if category_filter_type == CATEGORY_FILTER_TYPES[0] and selected_category:
        category_to_filter = selected_category
        subcategory_to_filter = session_state.get(SESSION_KEYS['selected_product_subcategory'], None)
        sub_subcategory_to_filter = session_state.get(SESSION_KEYS['selected_product_sub_subcategory'], None)
    elif category_filter_type == CATEGORY_FILTER_TYPES[1]:
        category_to_filter = session_state.get(SESSION_KEYS['selected_product_category'], None)
        subcategory_to_filter = session_state.get(SESSION_KEYS['selected_product_subcategory'], None)
        sub_subcategory_to_filter = session_state.get(SESSION_KEYS['selected_product_sub_subcategory'], None)
    
    return category_to_filter, subcategory_to_filter, sub_subcategory_to_filter


def _apply_category_filter(df: pd.DataFrame, category: Optional[str],
                           subcategory: Optional[str], sub_subcategory: Optional[str]) -> pd.DataFrame:
    """
    Применение фильтра по категории к DataFrame
    
    Args:
        df: DataFrame для фильтрации
        category: Категория
        subcategory: Подкатегория
        sub_subcategory: Под-подкатегория
        
    Returns:
        Отфильтрованный DataFrame
    """
    if df.empty or not category:
        return df
    
    filtered_df = filter_by_subcategory(df, category, subcategory, sub_subcategory)
    
    if filtered_df.empty:
        filter_text = _get_filter_text(category, subcategory, sub_subcategory)
        st.warning(f"Нет данных для категории '{filter_text}'")
    else:
        filter_text = _get_filter_text(category, subcategory, sub_subcategory)
        st.info(f"Отфильтровано по категории: {filter_text}")
    
    return filtered_df


def _display_orders_metrics(orders_df: pd.DataFrame, processed_orders: pd.DataFrame):
    """
    Отображение метрик по заказам
    
    Args:
        orders_df: DataFrame с заказами
        processed_orders: Обработанный DataFrame с заказами
    """
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Всего заказов", len(orders_df))
    with col2:
        if 'totalPrice' in orders_df.columns:
            total_sum = orders_df['totalPrice'].sum()
            st.metric("Общая сумма", f"{total_sum:,.0f} ₽")
    with col3:
        if not processed_orders.empty:
            avg_orders = processed_orders['Количество заказов'].mean()
            st.metric("Среднее в день", f"{avg_orders:.1f}")
    with col4:
        if 'date' in orders_df.columns:
            unique_dates = orders_df['date'].dt.date.nunique()
            st.metric("Дней с заказами", unique_dates)


def _display_sales_metrics(sales_df: pd.DataFrame, processed_sales: pd.DataFrame):
    """
    Отображение метрик по продажам
    
    Args:
        sales_df: DataFrame с продажами
        processed_sales: Обработанный DataFrame с продажами
    """
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Всего продаж", len(sales_df))
    with col2:
        if 'totalPrice' in sales_df.columns:
            total_sum = sales_df['totalPrice'].sum()
            st.metric("Общая сумма", f"{total_sum:,.0f} ₽")
    with col3:
        if not processed_sales.empty:
            avg_sales = processed_sales['Количество продаж'].mean()
            st.metric("Среднее в день", f"{avg_sales:.1f}")
    with col4:
        if 'date' in sales_df.columns:
            unique_dates = sales_df['date'].dt.date.nunique()
            st.metric("Дней с продажами", unique_dates)


def _display_stocks_metrics(stocks_df: pd.DataFrame):
    """
    Отображение метрик по остаткам
    
    Args:
        stocks_df: DataFrame с остатками
    """
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Всего позиций", len(stocks_df))
    with col2:
        if 'quantity' in stocks_df.columns:
            total_quantity = stocks_df['quantity'].sum()
            st.metric("Общее количество", f"{total_quantity:,.0f}")
    with col3:
        if 'quantity' in stocks_df.columns:
            avg_quantity = stocks_df['quantity'].mean()
            st.metric("Среднее на позицию", f"{avg_quantity:.1f}")


def _display_supplies_metrics(supplies_df: pd.DataFrame):
    """
    Отображение метрик по поставкам
    
    Args:
        supplies_df: DataFrame с поставками
    """
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Всего поставок", len(supplies_df))
    with col2:
        if 'date' in supplies_df.columns:
            unique_dates = supplies_df['date'].dt.date.nunique()
            st.metric("Дней с поставками", unique_dates)


def _display_orders_data(wb_api: 'WildberriesAPI', date_from: datetime, date_to: datetime,
                         category_filter_type: str, selected_category: Optional[str],
                         all_data_for_categories: List[pd.DataFrame]) -> bool:
    """
    Отображение данных о заказах
    
    Args:
        wb_api: API клиент Wildberries
        date_from: Дата начала
        date_to: Дата окончания
        category_filter_type: Тип фильтра категории
        selected_category: Выбранная категория
        all_data_for_categories: Список всех загруженных данных для определения категорий
        
    Returns:
        True если данные загружены успешно
    """
    st.subheader("📦 Заказы")
    orders_df = wb_api.get_orders(
        date_from.strftime("%Y-%m-%d"),
        date_to.strftime("%Y-%m-%d")
    )
    
    if orders_df.empty:
        st.warning("Нет данных о заказах за выбранный период")
        return False
    
    all_data_for_categories.append(orders_df)
    
    # Применение фильтра по категории
    category_to_filter, subcategory_to_filter, sub_subcategory_to_filter = _get_category_filters(
        category_filter_type, selected_category, st.session_state
    )
    
    if category_to_filter:
        orders_df = _apply_category_filter(
            orders_df, category_to_filter, subcategory_to_filter, sub_subcategory_to_filter
        )
        if orders_df.empty:
            return False
    
    st.success(f"Загружено {len(orders_df)} заказов")
    
    # Обработка данных
    processed_orders = process_orders_data(orders_df.copy())
    
    # Метрики
    _display_orders_metrics(orders_df, processed_orders)
    
    # График заказов по дням
    if not processed_orders.empty:
        fig = px.line(
            processed_orders,
            x='Дата',
            y='Количество заказов',
            title="Динамика заказов по дням",
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Таблица с данными
    with st.expander("📋 Детальные данные о заказах"):
        st.dataframe(orders_df, use_container_width=True)
    
    return True


def _display_sales_data(wb_api: 'WildberriesAPI', date_from: datetime, date_to: datetime,
                       category_filter_type: str, selected_category: Optional[str],
                       all_data_for_categories: List[pd.DataFrame]) -> bool:
    """
    Отображение данных о продажах
    
    Args:
        wb_api: API клиент Wildberries
        date_from: Дата начала
        date_to: Дата окончания
        category_filter_type: Тип фильтра категории
        selected_category: Выбранная категория
        all_data_for_categories: Список всех загруженных данных для определения категорий
        
    Returns:
        True если данные загружены успешно
    """
    st.markdown("---")
    st.subheader("💰 Продажи")
    sales_df = wb_api.get_sales(
        date_from.strftime("%Y-%m-%d"),
        date_to.strftime("%Y-%m-%d")
    )
    
    if sales_df.empty:
        st.warning("Нет данных о продажах за выбранный период")
        return False
    
    all_data_for_categories.append(sales_df)
    
    # Применение фильтра по категории
    category_to_filter, subcategory_to_filter, sub_subcategory_to_filter = _get_category_filters(
        category_filter_type, selected_category, st.session_state
    )
    
    if category_to_filter:
        sales_df = _apply_category_filter(
            sales_df, category_to_filter, subcategory_to_filter, sub_subcategory_to_filter
        )
        if sales_df.empty:
            return False
    
    st.success(f"Загружено {len(sales_df)} продаж")
    
    # Обработка данных
    processed_sales = process_sales_data(sales_df.copy())
    
    # Метрики
    _display_sales_metrics(sales_df, processed_sales)
    
    # График продаж по дням
    if not processed_sales.empty:
        fig = px.line(
            processed_sales,
            x='Дата',
            y='Количество продаж',
            title="Динамика продаж по дням",
            markers=True,
            color_discrete_sequence=['green']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Таблица с данными
    with st.expander("📋 Детальные данные о продажах"):
        st.dataframe(sales_df, use_container_width=True)
    
    return True


def _display_stocks_data(wb_api: 'WildberriesAPI', date_from: datetime,
                        category_filter_type: str, selected_category: Optional[str],
                        all_data_for_categories: List[pd.DataFrame]) -> bool:
    """
    Отображение данных об остатках
    
    Args:
        wb_api: API клиент Wildberries
        date_from: Дата начала
        category_filter_type: Тип фильтра категории
        selected_category: Выбранная категория
        all_data_for_categories: Список всех загруженных данных для определения категорий
        
    Returns:
        True если данные загружены успешно
    """
    st.markdown("---")
    st.subheader("📦 Остатки товаров")
    stocks_df = wb_api.get_stocks(date_from.strftime("%Y-%m-%d"))
    
    if stocks_df.empty:
        st.warning("Нет данных об остатках")
        return False
    
    all_data_for_categories.append(stocks_df)
    
    # Применение фильтра по категории
    category_to_filter, subcategory_to_filter, sub_subcategory_to_filter = _get_category_filters(
        category_filter_type, selected_category, st.session_state
    )
    
    if category_to_filter:
        stocks_df = _apply_category_filter(
            stocks_df, category_to_filter, subcategory_to_filter, sub_subcategory_to_filter
        )
        if stocks_df.empty:
            return False
    
    st.success(f"Загружено {len(stocks_df)} позиций")
    
    # Метрики
    _display_stocks_metrics(stocks_df)
    
    # Таблица с данными
    with st.expander("📋 Детальные данные об остатках"):
        st.dataframe(stocks_df, use_container_width=True)
    
    return True


def _display_supplies_data(wb_api: 'WildberriesAPI', date_from: datetime) -> bool:
    """
    Отображение данных о поставках
    
    Args:
        wb_api: API клиент Wildberries
        date_from: Дата начала
        
    Returns:
        True если данные загружены успешно
    """
    st.markdown("---")
    st.subheader("🚚 Поставки")
    supplies_df = wb_api.get_supplies(date_from.strftime("%Y-%m-%d"))
    
    if supplies_df.empty:
        st.warning("Нет данных о поставках")
        return False
    
    st.success(f"Загружено {len(supplies_df)} поставок")
    
    # Метрики
    _display_supplies_metrics(supplies_df)
    
    # Таблица с данными
    with st.expander("📋 Детальные данные о поставках"):
        st.dataframe(supplies_df, use_container_width=True)
    
    return True


def _display_commissions_data(wb_api: 'WildberriesAPI') -> bool:
    """
    Отображение данных о комиссиях
    
    Args:
        wb_api: API клиент Wildberries
        
    Returns:
        True если данные загружены успешно
    """
    st.markdown("---")
    st.subheader("💳 Комиссии по категориям (сегодня)")
    commissions_df = wb_api.get_commissions_by_category_today()
    
    if commissions_df.empty:
        st.warning("Нет данных о комиссиях на сегодняшний день")
        return False
    
    st.success(f"Загружено данных по {len(commissions_df)} категориям")
    
    # Выбор категории для фильтрации с поиском
    selected_category = None
    filtered_commissions_df = commissions_df.copy()
    
    if 'Категория' in commissions_df.columns:
        categories = ['Все категории'] + sorted(commissions_df['Категория'].unique().tolist())
        
        # Поисковая строка для категории
        search_category = st.text_input(
            "🔍 Поиск категории комиссии",
            key="commission_category_search",
            placeholder="Введите название категории для поиска..."
        )
        
        # Фильтрация категорий по поисковому запросу
        if search_category:
            filtered_categories = ['Все категории'] + [
                cat for cat in categories[1:] 
                if search_category.lower() in cat.lower()
            ]
            if not filtered_categories:
                filtered_categories = ['Все категории']
        else:
            filtered_categories = categories
        
        selected_category = st.selectbox(
            "Выберите категорию для отображения",
            options=filtered_categories,
            key="commission_category_filter",
            index=0
        )
        
        # Фильтрация данных по выбранной категории
        if selected_category != 'Все категории':
            filtered_commissions_df = commissions_df[
                commissions_df['Категория'] == selected_category
            ].copy()
    
    # Метрики
    col1, col2 = st.columns(2)
    with col1:
        if 'Сумма комиссии' in filtered_commissions_df.columns:
            total_commission = filtered_commissions_df['Сумма комиссии'].sum()
            st.metric("Общая сумма комиссии", f"{total_commission:,.2f} ₽")
        elif 'Средний процент комиссии' in filtered_commissions_df.columns:
            avg_commission = filtered_commissions_df['Средний процент комиссии'].mean()
            st.metric("Средний процент комиссии", f"{avg_commission:.2f}%")
        elif 'Процент комиссии' in filtered_commissions_df.columns:
            if selected_category and selected_category != 'Все категории' and len(filtered_commissions_df) == 1:
                commission_value = filtered_commissions_df['Процент комиссии'].iloc[0]
                st.metric("Процент комиссии", f"{commission_value:.2f}%")
            else:
                avg_commission = filtered_commissions_df['Процент комиссии'].mean()
                st.metric("Средний процент комиссии", f"{avg_commission:.2f}%")
    with col2:
        if 'Количество' in filtered_commissions_df.columns:
            total_quantity = filtered_commissions_df['Количество'].sum()
            st.metric("Общее количество товаров", f"{total_quantity:,.0f}")
        elif selected_category and selected_category != 'Все категории':
            st.metric("Количество категорий", "1")
        else:
            st.metric("Количество категорий", len(filtered_commissions_df))
    
    # График комиссий по категориям
    if not filtered_commissions_df.empty:
        title_suffix = f" - {selected_category}" if selected_category and selected_category != 'Все категории' else ""
        
        if 'Сумма комиссии' in filtered_commissions_df.columns:
            fig = px.bar(
                filtered_commissions_df,
                x='Категория',
                y='Сумма комиссии',
                title=f"Комиссии по категориям на сегодня{title_suffix}",
                color='Сумма комиссии',
                color_continuous_scale='Reds'
            )
        elif 'Средний процент комиссии' in filtered_commissions_df.columns:
            fig = px.bar(
                filtered_commissions_df,
                x='Категория',
                y='Средний процент комиссии',
                title=f"Средний процент комиссии по категориям на сегодня{title_suffix}",
                color='Средний процент комиссии',
                color_continuous_scale='Reds'
            )
        elif 'Процент комиссии' in filtered_commissions_df.columns:
            if selected_category and selected_category != 'Все категории' and len(filtered_commissions_df) == 1:
                commission_value = filtered_commissions_df['Процент комиссии'].iloc[0]
                st.info(f"**Процент комиссии для категории '{selected_category}': {commission_value:.2f}%**")
                return True
            else:
                fig = px.bar(
                    filtered_commissions_df,
                    x='Категория',
                    y='Процент комиссии',
                    title=f"Процент комиссии по категориям на сегодня{title_suffix}",
                    color='Процент комиссии',
                    color_continuous_scale='Reds'
                )
        else:
            return True
        
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Таблица с данными
    with st.expander("📋 Детальные данные о комиссиях"):
        st.dataframe(filtered_commissions_df, use_container_width=True)
    
    return True


def _display_logistics_data(wb_api: 'WildberriesAPI', date_from: datetime) -> bool:
    """
    Отображение данных о логистике
    
    Args:
        wb_api: API клиент Wildberries
        date_from: Дата начала
        
    Returns:
        True если данные загружены успешно
    """
    st.markdown("---")
    st.subheader("🚚 Логистика по складам")
    logistics_df = wb_api.get_logistics_by_warehouse_today(date_from.strftime("%Y-%m-%d"))
    
    if logistics_df.empty:
        st.warning(f"Нет данных о логистике на {date_from.strftime('%Y-%m-%d')}")
        return False
    
    st.success(f"Загружено данных по {len(logistics_df)} складам")
    
    # Выбор склада для фильтрации с поиском
    selected_warehouse = None
    filtered_logistics_df = logistics_df.copy()
    
    if 'Склад' in logistics_df.columns:
        warehouses = ['Все склады'] + sorted(logistics_df['Склад'].unique().tolist())
        
        # Поисковая строка для склада
        search_warehouse = st.text_input(
            "🔍 Поиск склада логистики",
            key="logistics_warehouse_search",
            placeholder="Введите название склада для поиска..."
        )
        
        # Фильтрация складов по поисковому запросу
        if search_warehouse:
            filtered_warehouses = ['Все склады'] + [
                wh for wh in warehouses[1:] 
                if search_warehouse.lower() in wh.lower()
            ]
            if not filtered_warehouses:
                filtered_warehouses = ['Все склады']
        else:
            filtered_warehouses = warehouses
        
        selected_warehouse = st.selectbox(
            "Выберите склад для отображения",
            options=filtered_warehouses,
            key="logistics_warehouse_filter",
            index=0
        )
        
        # Фильтрация данных по выбранному складу
        if selected_warehouse != 'Все склады':
            filtered_logistics_df = logistics_df[
                logistics_df['Склад'] == selected_warehouse
            ].copy()
    
    # Метрики
    col1, col2, col3 = st.columns(3)
    with col1:
        if 'Логистика за 1 ед. (₽)' in filtered_logistics_df.columns:
            avg_logistics = filtered_logistics_df['Логистика за 1 ед. (₽)'].mean()
            st.metric("Средняя логистика за 1 ед.", f"{avg_logistics:.2f} ₽")
        elif 'Логистика (₽)' in filtered_logistics_df.columns:
            avg_logistics = filtered_logistics_df['Логистика (₽)'].mean()
            st.metric("Средняя логистика", f"{avg_logistics:.2f} ₽")
    with col2:
        if 'Общая логистика (₽)' in filtered_logistics_df.columns:
            total_logistics = filtered_logistics_df['Общая логистика (₽)'].sum()
            st.metric("Общая логистика", f"{total_logistics:,.2f} ₽")
    with col3:
        if 'Количество товара' in filtered_logistics_df.columns:
            total_quantity = filtered_logistics_df['Количество товара'].sum()
            st.metric("Общее количество товара", f"{total_quantity:,.0f}")
        else:
            st.metric("Количество складов", len(filtered_logistics_df))
    
    # График логистики по складам
    date_str = date_from.strftime("%Y-%m-%d")
    title_suffix = f" - {selected_warehouse}" if selected_warehouse and selected_warehouse != 'Все склады' else ""
    
    if 'Логистика за 1 ед. (₽)' in filtered_logistics_df.columns:
        fig = px.bar(
            filtered_logistics_df,
            x='Склад',
            y='Логистика за 1 ед. (₽)',
            title=f"Логистика по складам на {date_str} (за 1 единицу товара){title_suffix}",
            color='Логистика за 1 ед. (₽)',
            color_continuous_scale='Blues'
        )
    elif 'Логистика (₽)' in filtered_logistics_df.columns:
        fig = px.bar(
            filtered_logistics_df,
            x='Склад',
            y='Логистика (₽)',
            title=f"Логистика по складам на {date_str}{title_suffix}",
            color='Логистика (₽)',
            color_continuous_scale='Blues'
        )
    else:
        return True
    
    fig.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Таблица с данными
    with st.expander("📋 Детальные данные о логистике"):
        st.dataframe(filtered_logistics_df, use_container_width=True)
    
    return True


def get_subcategories(df: pd.DataFrame, parent_category: str, level: int = 1) -> List[str]:
    """
    Получение списка подкатегорий для выбранной родительской категории
    
    Args:
        df: DataFrame для извлечения категорий
        parent_category: Родительская категория
        level: Уровень подкатегории (1 - первая подкатегория, 2 - вторая и т.д.)
        
    Returns:
        Список уникальных подкатегорий
    """
    if df.empty or not parent_category:
        return []
    
    category_col = get_category_column(df)
    if category_col is None:
        return []
    
    categories = df[category_col].dropna().unique().tolist()
    subcategories = set()
    
    for cat in categories:
        hierarchy = parse_category_hierarchy(str(cat))
        # Проверяем, что родительская категория совпадает с первым уровнем
        if len(hierarchy) > 0 and hierarchy[0].lower() == parent_category.lower():
            if len(hierarchy) > level:
                subcategories.add(hierarchy[level])
    
    return sorted(list(subcategories))


def filter_by_subcategory(df: pd.DataFrame, category: Optional[str] = None, 
                         subcategory: Optional[str] = None,
                         sub_subcategory: Optional[str] = None) -> pd.DataFrame:
    """
    Фильтрация DataFrame по категории и подкатегории товара
    
    Args:
        df: DataFrame для фильтрации
        category: Название категории для фильтрации (None - без фильтрации)
        subcategory: Название подкатегории для фильтрации (None - без фильтрации)
        sub_subcategory: Название под-подкатегории для фильтрации (None - без фильтрации)
        
    Returns:
        Отфильтрованный DataFrame
    """
    if df.empty:
        return df
    
    category_col = get_category_column(df)
    if category_col is None:
        return df
    
    filtered_df = df.copy()
    
    # Фильтрация по категории
    if category:
        # Фильтруем по первому уровню категории
        def matches_category(cat_str):
            hierarchy = parse_category_hierarchy(str(cat_str))
            return len(hierarchy) > 0 and hierarchy[0].lower() == category.lower()
        
        filtered_df = filtered_df[filtered_df[category_col].apply(matches_category)].copy()
    
    # Фильтрация по подкатегории
    if subcategory and not filtered_df.empty:
        # Фильтруем по второму уровню категории
        def matches_subcategory(cat_str):
            hierarchy = parse_category_hierarchy(str(cat_str))
            return len(hierarchy) > 1 and hierarchy[1].lower() == subcategory.lower()
        
        filtered_df = filtered_df[filtered_df[category_col].apply(matches_subcategory)].copy()
    
    # Фильтрация по под-подкатегории
    if sub_subcategory and not filtered_df.empty:
        # Фильтруем по третьему уровню категории
        def matches_sub_subcategory(cat_str):
            hierarchy = parse_category_hierarchy(str(cat_str))
            return len(hierarchy) > 2 and hierarchy[2].lower() == sub_subcategory.lower()
        
        filtered_df = filtered_df[filtered_df[category_col].apply(matches_sub_subcategory)].copy()
    
    return filtered_df


def _setup_sidebar_settings() -> tuple:
    """
    Настройка боковой панели с параметрами
    
    Returns:
        Кортеж (date_from, date_to, category_filter_type, selected_category, 
                load_orders, load_sales, load_stocks, load_supplies, 
                load_commissions, load_logistics, load_data)
    """
    st.header("⚙️ Настройки")
    st.markdown("---")
    
    # Выбор периода
    st.subheader("📅 Период анализа")
    date_from = st.date_input(
        "Дата начала",
        value=datetime.now() - timedelta(days=DEFAULT_DATE_RANGE_DAYS),
        max_value=datetime.now()
    )
    date_to = st.date_input(
        "Дата окончания",
        value=datetime.now(),
        max_value=datetime.now()
    )
    
    if date_from > date_to:
        st.error("Дата начала не может быть больше даты окончания")
        st.stop()
    
    st.markdown("---")
    
    # Выбор категории товара
    st.subheader("🏷️ Фильтр по категории товара")
    category_filter_type = st.radio(
        "Тип фильтра категории",
        CATEGORY_FILTER_TYPES,
        key=SESSION_KEYS['category_filter_type'],
        help="Выберите способ фильтрации по категории товара"
    )
    
    selected_category = None
    selected_subcategory = None
    
    if category_filter_type == CATEGORY_FILTER_TYPES[0]:
        selected_category = st.selectbox(
            "Выберите категорию товара",
            options=POPULAR_CATEGORIES,
            key="product_category_filter",
            index=0
        )
        if selected_category == "Все категории":
            selected_category = None
            if SESSION_KEYS['selected_product_subcategory'] in st.session_state:
                del st.session_state[SESSION_KEYS['selected_product_subcategory']]
        else:
            st.info("💡 Подкатегория будет доступна после загрузки данных")
            selected_subcategory = st.session_state.get(SESSION_KEYS['selected_product_subcategory'], None)
    else:
        st.info("Категория будет доступна после загрузки данных")
        selected_category = st.session_state.get(SESSION_KEYS['selected_product_category'], None)
        selected_subcategory = st.session_state.get(SESSION_KEYS['selected_product_subcategory'], None)
    
    st.markdown("---")
    
    # Выбор данных для загрузки
    st.subheader("📥 Данные для загрузки")
    load_orders = st.checkbox("Заказы", value=True)
    load_sales = st.checkbox("Продажи", value=True)
    load_stocks = st.checkbox("Остатки", value=False)
    load_supplies = st.checkbox("Поставки", value=False)
    load_commissions = st.checkbox("Комиссии по категориям (сегодня)", value=True)
    load_logistics = st.checkbox("Логистика по складам", value=True)
    
    st.markdown("---")
    
    # Кнопка загрузки данных
    load_data = st.button("🔄 Загрузить данные", type="primary")
    
    return (date_from, date_to, category_filter_type, selected_category,
            load_orders, load_sales, load_stocks, load_supplies,
            load_commissions, load_logistics, load_data)


def _display_category_selector(all_data_for_categories: List[pd.DataFrame],
                               category_filter_type: str, selected_category: Optional[str]):
    """
    Отображение селектора категорий из загруженных данных
    
    Args:
        all_data_for_categories: Список всех загруженных данных
        category_filter_type: Тип фильтра категории
        selected_category: Выбранная категория
    """
    if category_filter_type != CATEGORY_FILTER_TYPES[1] or not all_data_for_categories:
        return
    
    st.markdown("---")
    st.subheader("🏷️ Выбор категории из загруженных данных")
    
    # Собираем все категории из загруженных данных
    all_categories = set()
    for df in all_data_for_categories:
        categories = get_available_categories(df)
        all_categories.update(categories)
    
    if not all_categories:
        st.warning("Не удалось определить категории в загруженных данных. Возможно, в данных нет поля с категорией товара.")
        return
    
    # Получаем первый уровень категорий (основные категории)
    first_level_categories = set()
    for cat in all_categories:
        hierarchy = parse_category_hierarchy(str(cat))
        if len(hierarchy) > 0:
            first_level_categories.add(hierarchy[0])
    
    categories_list = ['Все категории'] + sorted(list(first_level_categories))
    
    # Поисковая строка для категории
    search_category = st.text_input(
        "🔍 Поиск категории товара",
        key="product_category_search",
        placeholder="Введите название категории для поиска..."
    )
    
    # Фильтрация категорий по поисковому запросу
    if search_category:
        filtered_categories = ['Все категории'] + [
            cat for cat in categories_list[1:] 
            if search_category.lower() in cat.lower()
        ]
        if not filtered_categories:
            filtered_categories = ['Все категории']
    else:
        filtered_categories = categories_list
    
    selected_data_category = st.selectbox(
        "Выберите категорию товара",
        options=filtered_categories,
        key="selected_product_category_from_data",
        index=0 if SESSION_KEYS['selected_product_category'] not in st.session_state else 
              next((i for i, cat in enumerate(filtered_categories) 
                    if cat == st.session_state.get(SESSION_KEYS['selected_product_category'])), 0)
    )
    
    if selected_data_category != 'Все категории':
        st.session_state[SESSION_KEYS['selected_product_category']] = selected_data_category
        
        # Получаем подкатегории для выбранной категории
        subcategories = set()
        for df in all_data_for_categories:
            subcats = get_subcategories(df, selected_data_category, level=1)
            subcategories.update(subcats)
        
        if subcategories:
            subcategories_list = ['Все подкатегории'] + sorted(list(subcategories))
            
            selected_data_subcategory = st.selectbox(
                "Выберите подкатегорию товара (например, белье)",
                options=subcategories_list,
                key="selected_product_subcategory_from_data",
                index=0 if SESSION_KEYS['selected_product_subcategory'] not in st.session_state else 
                      next((i for i, subcat in enumerate(subcategories_list) 
                            if subcat == st.session_state.get(SESSION_KEYS['selected_product_subcategory'])), 0)
            )
            
            if selected_data_subcategory != 'Все подкатегории':
                st.session_state[SESSION_KEYS['selected_product_subcategory']] = selected_data_subcategory
                
                # Получаем под-подкатегории
                sub_subcategories = set()
                for df in all_data_for_categories:
                    filtered_df = filter_by_subcategory(df, selected_data_category, selected_data_subcategory)
                    if not filtered_df.empty:
                        category_col = get_category_column(filtered_df)
                        if category_col:
                            for cat in filtered_df[category_col].dropna().unique():
                                hierarchy = parse_category_hierarchy(str(cat))
                                if len(hierarchy) > 2:
                                    sub_subcategories.add(hierarchy[2])
                
                if sub_subcategories:
                    sub_subcategories_list = ['Все под-подкатегории'] + sorted(list(sub_subcategories))
                    
                    selected_data_sub_subcategory = st.selectbox(
                        "Выберите под-подкатегорию товара (например, боди)",
                        options=sub_subcategories_list,
                        key="selected_product_sub_subcategory_from_data",
                        index=0 if SESSION_KEYS['selected_product_sub_subcategory'] not in st.session_state else 
                              next((i for i, subsubcat in enumerate(sub_subcategories_list) 
                                    if subsubcat == st.session_state.get(SESSION_KEYS['selected_product_sub_subcategory'])), 0)
                    )
                    
                    if selected_data_sub_subcategory != 'Все под-подкатегории':
                        st.session_state[SESSION_KEYS['selected_product_sub_subcategory']] = selected_data_sub_subcategory
                        filter_text = _get_filter_text(selected_data_category, selected_data_subcategory, selected_data_sub_subcategory)
                        st.info(f"💡 Выбрана категория: {filter_text}. Перезагрузите данные для применения фильтра.")
                    else:
                        if SESSION_KEYS['selected_product_sub_subcategory'] in st.session_state:
                            del st.session_state[SESSION_KEYS['selected_product_sub_subcategory']]
                        filter_text = _get_filter_text(selected_data_category, selected_data_subcategory)
                        st.info(f"💡 Выбрана категория: {filter_text}. Перезагрузите данные для применения фильтра.")
                else:
                    if SESSION_KEYS['selected_product_sub_subcategory'] in st.session_state:
                        del st.session_state[SESSION_KEYS['selected_product_sub_subcategory']]
                    filter_text = _get_filter_text(selected_data_category, selected_data_subcategory)
                    st.info(f"💡 Выбрана категория: {filter_text}. Перезагрузите данные для применения фильтра.")
            else:
                for key in [SESSION_KEYS['selected_product_subcategory'], SESSION_KEYS['selected_product_sub_subcategory']]:
                    if key in st.session_state:
                        del st.session_state[key]
                st.info(f"💡 Выбрана категория: {selected_data_category}. Перезагрузите данные для применения фильтра.")
        else:
            for key in [SESSION_KEYS['selected_product_subcategory'], SESSION_KEYS['selected_product_sub_subcategory']]:
                if key in st.session_state:
                    del st.session_state[key]
            st.info(f"💡 Выбрана категория: {selected_data_category}. Перезагрузите данные для применения фильтра.")
    else:
        st.session_state[SESSION_KEYS['selected_product_category']] = None
        for key in [SESSION_KEYS['selected_product_subcategory'], SESSION_KEYS['selected_product_sub_subcategory']]:
            if key in st.session_state:
                del st.session_state[key]


def _display_preset_category_selector(all_data_for_categories: List[pd.DataFrame],
                                      selected_category: Optional[str]):
    """
    Отображение селектора подкатегорий для предустановленной категории
    
    Args:
        all_data_for_categories: Список всех загруженных данных
        selected_category: Выбранная категория
    """
    if not selected_category or not all_data_for_categories:
        return
    
    st.markdown("---")
    st.subheader("🏷️ Выбор подкатегории для предустановленной категории")
    
    # Получаем подкатегории для выбранной предустановленной категории
    subcategories = set()
    for df in all_data_for_categories:
        subcats = get_subcategories(df, selected_category, level=1)
        subcategories.update(subcats)
    
    if not subcategories:
        for key in [SESSION_KEYS['selected_product_subcategory'], SESSION_KEYS['selected_product_sub_subcategory']]:
            if key in st.session_state:
                del st.session_state[key]
        st.info(f"💡 Выбрана категория: {selected_category}. Подкатегории не найдены в загруженных данных.")
        return
    
    subcategories_list = ['Все подкатегории'] + sorted(list(subcategories))
    
    selected_preset_subcategory = st.selectbox(
        "Выберите подкатегорию товара (например, белье)",
        options=subcategories_list,
        key="selected_product_subcategory_preset",
        index=0 if SESSION_KEYS['selected_product_subcategory'] not in st.session_state else 
              next((i for i, subcat in enumerate(subcategories_list) 
                    if subcat == st.session_state.get(SESSION_KEYS['selected_product_subcategory'])), 0)
    )
    
    if selected_preset_subcategory != 'Все подкатегории':
        st.session_state[SESSION_KEYS['selected_product_subcategory']] = selected_preset_subcategory
        
        # Получаем под-подкатегории
        sub_subcategories = set()
        for df in all_data_for_categories:
            filtered_df = filter_by_subcategory(df, selected_category, selected_preset_subcategory)
            if not filtered_df.empty:
                category_col = get_category_column(filtered_df)
                if category_col:
                    for cat in filtered_df[category_col].dropna().unique():
                        hierarchy = parse_category_hierarchy(str(cat))
                        if len(hierarchy) > 2:
                            sub_subcategories.add(hierarchy[2])
        
        if sub_subcategories:
            sub_subcategories_list = ['Все под-подкатегории'] + sorted(list(sub_subcategories))
            
            selected_preset_sub_subcategory = st.selectbox(
                "Выберите под-подкатегорию товара (например, боди)",
                options=sub_subcategories_list,
                key="selected_product_sub_subcategory_preset",
                index=0 if SESSION_KEYS['selected_product_sub_subcategory'] not in st.session_state else 
                      next((i for i, subsubcat in enumerate(sub_subcategories_list) 
                            if subsubcat == st.session_state.get(SESSION_KEYS['selected_product_sub_subcategory'])), 0)
            )
            
            if selected_preset_sub_subcategory != 'Все под-подкатегории':
                st.session_state[SESSION_KEYS['selected_product_sub_subcategory']] = selected_preset_sub_subcategory
                filter_text = _get_filter_text(selected_category, selected_preset_subcategory, selected_preset_sub_subcategory)
                st.info(f"💡 Выбрана категория: {filter_text}. Перезагрузите данные для применения фильтра.")
            else:
                if SESSION_KEYS['selected_product_sub_subcategory'] in st.session_state:
                    del st.session_state[SESSION_KEYS['selected_product_sub_subcategory']]
                filter_text = _get_filter_text(selected_category, selected_preset_subcategory)
                st.info(f"💡 Выбрана категория: {filter_text}. Перезагрузите данные для применения фильтра.")
        else:
            if SESSION_KEYS['selected_product_sub_subcategory'] in st.session_state:
                del st.session_state[SESSION_KEYS['selected_product_sub_subcategory']]
            filter_text = _get_filter_text(selected_category, selected_preset_subcategory)
            st.info(f"💡 Выбрана категория: {filter_text}. Перезагрузите данные для применения фильтра.")
    else:
        for key in [SESSION_KEYS['selected_product_subcategory'], SESSION_KEYS['selected_product_sub_subcategory']]:
            if key in st.session_state:
                del st.session_state[key]
        st.info(f"💡 Выбрана категория: {selected_category}. Перезагрузите данные для применения фильтра.")


def show_auth_page(user_manager: UserManager):
    """Отображение страницы авторизации/регистрации"""
    st.title("🔐 Авторизация")
    
    tab1, tab2 = st.tabs(["Вход", "Регистрация"])
    
    with tab1:
        st.subheader("Вход в систему")
        username = st.text_input("Логин", key="login_username")
        password = st.text_input("Пароль", type="password", key="login_password")
        
        if st.button("Войти", type="primary", key="login_button"):
            if username and password:
                if user_manager.authenticate_user(username, password):
                    st.session_state['authenticated'] = True
                    st.session_state['username'] = username
                    st.session_state['user_id'] = user_manager.get_user_id(username)
                    st.success("Успешный вход!")
                    st.rerun()
                else:
                    st.error("Неверный логин или пароль")
            else:
                st.warning("Заполните все поля")
    
    with tab2:
        st.subheader("Регистрация нового пользователя")
        new_username = st.text_input("Логин", key="reg_username")
        new_password = st.text_input("Пароль", type="password", key="reg_password")
        confirm_password = st.text_input("Подтвердите пароль", type="password", key="reg_confirm_password")
        
        if st.button("Зарегистрироваться", type="primary", key="register_button"):
            if new_username and new_password and confirm_password:
                if new_password != confirm_password:
                    st.error("Пароли не совпадают")
                elif len(new_password) < 6:
                    st.warning("Пароль должен содержать минимум 6 символов")
                else:
                    if user_manager.register_user(new_username, new_password):
                        st.success("Регистрация успешна! Теперь вы можете войти.")
                    else:
                        st.error("Пользователь с таким логином уже существует")
            else:
                st.warning("Заполните все поля")


def show_api_keys_management(user_manager: UserManager, user_id: int):
    """Отображение управления API ключами"""
    st.subheader("🔑 Управление API ключами")
    
    # Получение списка API ключей
    api_keys = user_manager.get_api_keys(user_id)
    
    # Форма добавления нового API ключа
    with st.expander("➕ Добавить новый API ключ", expanded=len(api_keys) == 0):
        new_key_name = st.text_input("Наименование API ключа", key="new_key_name", 
                                     placeholder="Например: Основной аккаунт, Тестовый аккаунт")
        new_api_key = st.text_input("API ключ Wildberries", type="password", key="new_api_key")
        
        if st.button("Добавить API ключ", type="primary", key="add_key_button"):
            if new_key_name and new_api_key:
                if user_manager.add_api_key(user_id, new_key_name, new_api_key):
                    st.success("API ключ успешно добавлен!")
                    st.rerun()
                else:
                    st.error("API ключ с таким наименованием уже существует")
            else:
                st.warning("Заполните все поля")
    
    # Список существующих API ключей
    if api_keys:
        st.markdown("### Ваши API ключи")
        
        # Выбор активного API ключа
        key_options = {f"{key['name']} (ID: {key['id']})": key['id'] for key in api_keys}
        selected_key_name = st.selectbox(
            "Выберите API ключ для работы",
            options=list(key_options.keys()),
            key="api_key_selector",
            index=0 if 'selected_api_key_id' not in st.session_state else 
                  next((i for i, (name, key_id) in enumerate(key_options.items()) 
                        if key_id == st.session_state.get('selected_api_key_id')), 0)
        )
        
        selected_key_id = key_options[selected_key_name]
        st.session_state['selected_api_key_id'] = selected_key_id
        
        # Получение выбранного API ключа
        selected_api_key = user_manager.get_api_key_by_id(user_id, selected_key_id)
        
        # Таблица с API ключами
        st.markdown("---")
        st.markdown("#### Список всех API ключей")
        
        for key in api_keys:
            col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
            with col1:
                is_active = "✅ Активен" if key['id'] == selected_key_id else ""
                st.write(f"**{key['name']}** {is_active}")
            with col2:
                masked_key = key['api_key'][:10] + "..." + key['api_key'][-4:] if len(key['api_key']) > 14 else "***"
                st.write(f"`{masked_key}`")
            with col3:
                if st.button("Использовать", key=f"use_key_{key['id']}"):
                    st.session_state['selected_api_key_id'] = key['id']
                    st.rerun()
            with col4:
                if st.button("🗑️", key=f"delete_key_{key['id']}"):
                    if user_manager.delete_api_key(user_id, key['id']):
                        st.success("API ключ удален")
                        if st.session_state.get('selected_api_key_id') == key['id']:
                            del st.session_state['selected_api_key_id']
                        st.rerun()
                    else:
                        st.error("Ошибка при удалении")
        
        return selected_api_key
    else:
        st.info("У вас пока нет сохраненных API ключей. Добавьте первый ключ выше.")
        return None


def create_dashboard():
    """Создание интерактивного дашборда"""
    
    st.set_page_config(
        page_title="Статистика Wildberries",
        page_icon="📊",
        layout="wide"
    )
    
    # Инициализация менеджера пользователей
    user_manager = UserManager()
    
    # Проверка аутентификации
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False
    
    # Если не авторизован, показываем страницу авторизации
    if not st.session_state.get('authenticated'):
        show_auth_page(user_manager)
        st.stop()
    
    # Получение информации о пользователе
    username = st.session_state.get('username')
    user_id = st.session_state.get('user_id')
    
    # Боковая панель с информацией о пользователе, управлением API ключами и настройками
    with st.sidebar:
        st.header(f"👤 {username}")
        
        if st.button("Выйти", key="logout_button"):
            for key in [SESSION_KEYS['authenticated'], SESSION_KEYS['username'], 
                       SESSION_KEYS['user_id'], SESSION_KEYS['selected_api_key_id']]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        
        st.markdown("---")
        
        # Управление API ключами
        selected_api_key = show_api_keys_management(user_manager, user_id)
        
        st.markdown("---")
        
        # Настройки дашборда
        (date_from, date_to, category_filter_type, selected_category,
         load_orders, load_sales, load_stocks, load_supplies,
         load_commissions, load_logistics, load_data) = _setup_sidebar_settings()
    
    # Основной контент
    st.title("📊 Дашборд статистики Wildberries")
    st.markdown("---")
    
    # Проверка наличия API ключа
    if not selected_api_key:
        st.warning("⚠️ Пожалуйста, добавьте и выберите API ключ для продолжения")
        st.info("💡 API ключ можно получить в личном кабинете поставщика Wildberries")
        st.stop()
    
    st.markdown("---")
    
    # Инициализация API клиента
    wb_api = WildberriesAPI(selected_api_key)
    
    if load_data:
        with st.spinner("Загрузка данных..."):
            # Загрузка данных
            data_loaded = False
            
            # Сбор всех данных для определения доступных категорий
            all_data_for_categories = []
            
            if load_orders:
                if _display_orders_data(wb_api, date_from, date_to, category_filter_type, 
                                       selected_category, all_data_for_categories):
                    data_loaded = True
                
            if load_sales:
                if _display_sales_data(wb_api, date_from, date_to, category_filter_type,
                                     selected_category, all_data_for_categories):
                    data_loaded = True
            
            if load_stocks:
                if _display_stocks_data(wb_api, date_from, category_filter_type,
                                       selected_category, all_data_for_categories):
                    data_loaded = True
            
            if load_supplies:
                if _display_supplies_data(wb_api, date_from):
                    data_loaded = True
            
            if load_commissions:
                if _display_commissions_data(wb_api):
                    data_loaded = True
            
            if load_logistics:
                if _display_logistics_data(wb_api, date_from):
                    data_loaded = True
            
            # Отображение селекторов категорий
            if data_loaded:
                _display_category_selector(all_data_for_categories, category_filter_type, selected_category)
                _display_preset_category_selector(all_data_for_categories, selected_category)
            
            if not data_loaded:
                st.info("Выберите хотя бы один тип данных для загрузки")
    else:
        st.info("👈 Настройте параметры в боковой панели и нажмите 'Загрузить данные'")


if __name__ == "__main__":
    create_dashboard()


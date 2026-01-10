import os
import random
import json
from datetime import datetime, timedelta, time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, ContextTypes, CallbackQueryHandler,
    MessageHandler, filters
)
from dotenv import load_dotenv
import redis
import logging

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

DATE_THEMES = [
    "Время года 🌸🍂❄️🌞",
    "Экстрим 🪂🏄‍♂️🧗‍♀️",
    "Вода 🌊🏊‍♂️⛵",
    "Головоломка 🧩♟️🤔",
    "Романтика 💕🌹🕯️",
    "Аромат 🌸🍋🕯️",
    "Путешествие ✈️🗺️🧳",
    "Азарт 🎲♠️🎰",
    "Взаимодействие, тактильность 🤝🔊💆‍♂️",
    "Высота 🏔️🏙️🎢",
    "Дом 🏠🍳🛋️",
    "Искусство 🎨🖼️🎭",
    "Наука 🔬⚗️🧪",
    "Вкус 🍽️🍷🍓",
    "Детство 🧸🎈🪀",
    "Шум 🔊🎵🎸"
]

month_names = {
    1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля',
    5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа',
    9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
}

class MonthlyMeetBot:
    def __init__(self):
        self.token = os.getenv('BOT_TOKEN')
        self.redis_url = os.getenv('REDIS_URL')
        self.redis_client = redis.from_url(
            self.redis_url,
            socket_connect_timeout=10,
            socket_timeout=10,
            retry_on_timeout=True,
            max_connections=10,
            health_check_interval=30
        )
        self.connect_redis()
        
    def connect_redis(self):
        try:
            self.redis_client.ping()
            logger.info("✅ Redis подключение успешно")
        except redis.ConnectionError as e:
            logger.error(f"❌ Ошибка подключения Redis: {e}")

    async def keep_redis_awake(self, context: ContextTypes.DEFAULT_TYPE = None):
        try:
            if self.redis_client:
                self.redis_client.ping()
                logger.info("✅ Redis ping successful")
                return True
        except Exception as e:
            logger.warning(f"⚠️ Redis ping failed: {e}")
            self.connect_redis()
            return False
        
    def get_user_data(self, user_id):
        data = self.redis_client.get(f'user:{user_id}')
        return json.loads(data) if data else None
    
    def set_user_data(self, user_id, data):
        self.redis_client.set(f'user:{user_id}', json.dumps(data))
    
    def get_pair_data(self, pair_id):
        data = self.redis_client.get(f'pair:{pair_id}')
        return json.loads(data) if data else None
    
    def set_pair_data(self, pair_id, data):
        self.redis_client.set(f'pair:{pair_id}', json.dumps(data))
    
    def get_all_pairs(self):
        """Получить все пары"""
        pairs = []
        for key in self.redis_client.scan_iter('pair:*'):
            if isinstance(key, bytes):
                key_str = key.decode('utf-8')
            else:
                key_str = str(key)
            
            pair_id = key_str.split(':', 1)[1] if ':' in key_str else key_str
            pair_data = self.get_pair_data(pair_id)

            if pair_data:
                pairs.append(pair_data)
        
        return pairs
    
    def get_user_by_username(self, username):
        """Найти пользователя по username"""
        username = username.lower().replace('@', '')
        for key in self.redis_client.scan_iter('user:*'):
            user_data = json.loads(self.redis_client.get(key))
            if user_data.get('username', '').lower().replace('@', '') == username:
                return user_data
        return None
    
    def get_random_theme(self):
        """Выбор случайной темы для свидания"""
        return random.choice(DATE_THEMES)
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        user_data = self.get_user_data(user.id)
        if not user_data:
            user_data = {
                'id': user.id,
                'username': user.username,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'partner_id': None,
                'pending_requests': [],
                'pair_id': None,
                'registered_at': datetime.now().isoformat()
            }
            self.set_user_data(user.id, user_data)
            logger.info(f"✅ Новый пользователь зарегистрирован: {user.id}")
        
        help_text = (
            f"👋 Привет, {user.first_name}!\n"
            "Я бот для организации регулярных свиданий\n\n"
            "📍 *Доступные команды:*\n"
            "*/start* - Начать работу со мной\n"
            "*/partner* - Указать партнера для создания пары\n"
            "*/status* - Посмотреть текущий статус и информацию о паре\n"
            "*/cancel* - Отмена действия\n"
            "*/help* - Справка\n\n"
            "💡 *Как это работает:*\n"
            "• Используй */partner username*, чтобы создать пару\n"
            "• Партнер получит запрос и должен будет его подтвердить\n"
            "• После создания пары я каждый месяц буду:\n"
            "  - Выбирать организатора свидания\n"
            "  - Выбирать дату месяца, в которую нужно будет организовать свидание\n"
            "  - Выбирать тему для свидания\n"
            "  - Уведомлять организатора\n"
            "• Организатор должен устроить свидание на выбранную мной тему в назначенный мной день\n\n"
            "🧡 *Наслаждайтесь регулярными свиданиями!*"
        )
        
        await update.message.reply_text(help_text, parse_mode='markdown')

    async def partner_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_data = self.get_user_data(user.id)
        
        if not user_data:
            await update.message.reply_text("❌ Сначала запусти бота командой /start")
            return
        
        if user_data.get('partner_id') and user_data.get('pair_id'):
            await update.message.reply_text("❌ У тебя уже есть партнер")
            return
        
        if not context.args:
            await update.message.reply_text(
                "👤 *Указание партнера*\n\n"
                "Отправь username партнера (с @)",
                parse_mode='markdown'
            )
            user_data['awaiting_partner_input'] = True
            self.set_user_data(user.id, user_data)
            return
        
        username = context.args[0]
        if not username.startswith('@'):
            username = '@' + username
        
        await self.process_partner_input(update, context, username)

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /status"""
        user = update.effective_user
        user_data = self.get_user_data(user.id)
        
        if not user_data:
            await update.message.reply_text("❌ Сначала запусти бота командой /start")
            return
        
        if user_data.get('partner_id') and user_data.get('pair_id'):
            partner_data = self.get_user_data(user_data['partner_id'])

            status_text = (
                f"📊 <b>Твой статус:</b>\n\n"
                f"👥 Пара с @{partner_data['username']}"
            )
            
            await update.message.reply_text(status_text, parse_mode='HTML')
        else:
            await update.message.reply_text(
                "❌ Ты еще не в паре\n\n"
                "Используй команду /partner, чтобы создать пару"
            )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подробная справка"""
        help_text = (
            "📍 *Доступные команды:*\n"
            "*/start* - Начать работу со мной\n"
            "*/partner* - Указать партнера для создания пары\n"
            "*/status* - Посмотреть текущий статус и информацию о паре\n"
            "*/cancel* - Отмена действия\n"
            "*/help* - Справка\n\n"
            "💡 *Как это работает:*\n"
            "• Используй */partner username*, чтобы создать пару\n"
            "• Партнер получит запрос и должен будет его подтвердить\n"
            "• После создания пары я каждый месяц буду:\n"
            "  - Выбирать организатора свидания\n"
            "  - Выбирать дату месяца, в которую нужно будет организовать свидание\n"
            "  - Выбирать тему для свидания\n"
            "  - Уведомлять организатора\n"
            "• Организатор должен устроить свидание на выбранную мной тему в назначенный мной день\n\n"
            "🧡 *Наслаждайтесь регулярными свиданиями!*"
        )
        
        await update.message.reply_text(help_text, parse_mode='markdown')

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        user_data = self.get_user_data(user.id)
        
        if not user_data:
            await query.edit_message_text("❌ Сначала запусти бота командой /start")
            return
        
        if query.data.startswith('accept_'):
            partner_id = int(query.data.split('_')[1])
            await self.accept_pair_request(query, user_data, partner_id, context)
        
        elif query.data.startswith('reject_'):
            partner_id = int(query.data.split('_')[1])
            await self.reject_pair_request(query, user_data, partner_id, context)
        
        else:
            await query.edit_message_text("❌ Неизвестная команда")
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        text = update.message.text
        
        user_data = self.get_user_data(user.id)
        if not user_data:
            await self.start(update, context)
            return
        
        if user_data.get('awaiting_partner_input'):
            if text.startswith('@'):
                await self.process_partner_input(update, context, text)
            else:
                await update.message.reply_text(
                    "Пожалуйста, укажи username партнера (с @)\n"
                    "Или отправь /cancel, чтобы отменить действие"
                )
        else:
            await update.message.reply_text(
                "🤖 *Используй команды:*\n"
                "/partner - Указать партнера\n"
                "/status - Посмотреть статус\n" 
                "/cancel - Отмена действия\n"
                "/help - Справка\n\n",
                parse_mode='markdown'
            )

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена текущего действия"""
        user = update.effective_user
        user_data = self.get_user_data(user.id)
        
        if not user_data:
            await update.message.reply_text("❌ Сначала запусти бота командой /start")
            return
        
        if user_data.get('awaiting_partner_input'):
            user_data['awaiting_partner_input'] = False
            self.set_user_data(user.id, user_data)
            await update.message.reply_text("✅ Ввод партнера отменен")
        else:
            await update.message.reply_text("❌ Нет активных действий для отмены")

    async def set_bot_commands(self, application):
        """Установка команд меню"""
        commands = [
            BotCommand("start", "Главное меню"),
            BotCommand("partner", "Указать партнера"),
            BotCommand("status", "Посмотреть статус"),
            BotCommand("cancel", "Отменить действие"),
            BotCommand("help", "Справка")
        ]
        await application.bot.set_my_commands(commands)
    
    async def process_partner_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, username: str):
        user = update.effective_user
        user_data = self.get_user_data(user.id)
        
        if user_data and user_data.get('awaiting_partner_input'):
            user_data['awaiting_partner_input'] = False
            self.set_user_data(user.id, user_data)
        
        partner_data = self.get_user_by_username(username)
        
        if not partner_data:
            bot_username = (await context.bot.get_me()).username
            invitation_text = (
                f"❌ Пользователь {username} не найден в системе.\n\n"
                f"📩 Отправь это приглашение своему партнеру:\n"
                f"Привет! Давай организовывать регулярные свидания! "
                f"Перейди по ссылке и введи /start, чтобы зарегистрироваться:\n"
                f"https://t.me/{bot_username}?start=invite_{user.id}\n\n"
            )
            
            await update.message.reply_text(invitation_text)
            return
        
        if partner_data['id'] == user.id:
            await update.message.reply_text("❌ Самовлюбленность — это хорошо, но в меру")
            return
        
        if user_data.get('partner_id'):
            await update.message.reply_text("❌ Я поддерживаю традиционные ценности, у тебя уже есть партнер")
            return
        
        if partner_data.get('partner_id'):
            await update.message.reply_text("❌ У этого пользователя уже есть партнер")
            return
        
        if 'pending_requests' not in partner_data:
            partner_data['pending_requests'] = []
        
        existing_request = next((req for req in partner_data['pending_requests'] 
                            if req['from_user_id'] == user.id), None)
        
        if existing_request:
            await update.message.reply_text("✅ Запрос уже отправлен! Ожидай подтверждения от партнера")
            return
        
        partner_data['pending_requests'].append({
            'from_user_id': user.id,
            'from_username': user.username,
            'timestamp': datetime.now().isoformat()
        })
        
        self.set_user_data(partner_data['id'], partner_data)
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Принять", callback_data=f'accept_{user.id}'),
                InlineKeyboardButton("❌ Отклонить", callback_data=f'reject_{user.id}')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await context.bot.send_message(
                chat_id=partner_data['id'],
                text=f"💌 Запрос на создание пары\n\n"
                    f"Пользователь @{user.username} хочет создать с тобой пару для регулярных свиданий\n\n"
                    f"Нажми одну из кнопок ниже, чтобы принять или отклонить запрос:",
                reply_markup=reply_markup
            )
            await update.message.reply_text("✅ Запрос отправлен! Ожидай подтверждения от партнера")
        except Exception as e:
            logger.error(f"Error sending request: {e}")
            await update.message.reply_text(
                "❌ Не удалось отправить запрос. Возможно, партнер заблокировал меня\n\n"
                f"Отправь партнеру пригласительную ссылку:\n"
                f"https://t.me/{(await context.bot.get_me()).username}?start=invite_{user.id}"
            )
    
    async def accept_pair_request(self, query, user_data, partner_id, context: ContextTypes.DEFAULT_TYPE):
        """Принять запрос на создание пары"""
        partner_data = self.get_user_data(partner_id)
        if not partner_data:
            await query.edit_message_text("❌ Пользователя больше не существует")
            return
        
        if user_data.get('partner_id'):
            await query.edit_message_text("❌ Я поддерживаю традиционные ценности, у тебя уже есть партнер")
            return
        
        if partner_data.get('partner_id'):
            await query.edit_message_text("❌ У этого пользователя уже есть партнер")
            return
        
        if 'pending_requests' in user_data:
            user_data['pending_requests'] = [
                req for req in user_data['pending_requests'] 
                if req['from_user_id'] != partner_id
            ]
        
        pair_id = f"{min(user_data['id'], partner_id)}_{max(user_data['id'], partner_id)}"
        
        pair_data = {
            'id': pair_id,
            'user1_id': min(user_data['id'], partner_id),
            'user2_id': max(user_data['id'], partner_id),
            'created_at': datetime.now().isoformat(),
            'next_date': None,
            'organizer': None,
            'theme': None
        }
        
        user_data['partner_id'] = partner_id
        user_data['pair_id'] = pair_id
        
        partner_data['partner_id'] = user_data['id']
        partner_data['pair_id'] = pair_id
        
        self.set_user_data(user_data['id'], user_data)
        self.set_user_data(partner_data['id'], partner_data)
        self.set_pair_data(pair_id, pair_data)
        
        await query.edit_message_text("✅ Пара создана! Теперь вы будете получать уведомления о свиданиях")
        
        try:
            await context.bot.send_message(
                chat_id=partner_id,
                text=f"🎉 Пользователь @{user_data['username']} принял твой запрос на создание пары!"
            )
        except Exception as e:
            logger.error(f"Error notifying partner: {e}")
    
    async def reject_pair_request(self, query, user_data, partner_id, context: ContextTypes.DEFAULT_TYPE):
        """Отклонить запрос на создание пары"""
        if 'pending_requests' in user_data:
            user_data['pending_requests'] = [
                req for req in user_data['pending_requests'] 
                if req['from_user_id'] != partner_id
            ]
            self.set_user_data(user_data['id'], user_data)
        
        await query.edit_message_text("❌ Запрос отклонен")

        try:
            await context.bot.send_message(
                chat_id=partner_id,
                text=f"❌ Пользователь @{user_data['username']} отклонил твой запрос на создание пары"
            )
        except Exception as e:
            logger.error(f"Error notifying partner: {e}")

    async def monthly_planning(self, context: ContextTypes.DEFAULT_TYPE):
        """Ежемесячное планирование свиданий"""
        try:
            logger.info("📅 Начало ежемесячного планирования свиданий")
            
            pairs = self.get_all_pairs()
            logger.info(f"📊 Найдено пар для планирования: {len(pairs)}")
            
            for pair_data in pairs:
                await self.plan_date_for_pair(pair_data, context.bot)
            
            logger.info("✅ Ежемесячное планирование завершено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка ежемесячного планирования: {e}")

    async def plan_date_for_pair(self, pair_data, bot):
        """Планирует свидание для одной пары"""
        try:
            pair_id = pair_data['id']
            
            organizer_id = random.choice([pair_data['user1_id'], pair_data['user2_id']])
            
            now = datetime.now()
            days_in_month = (datetime(now.year, now.month % 12 + 1, 1) - timedelta(days=1)).day
            selected_day = random.randint(4, days_in_month)
            
            theme = self.get_random_theme()
            
            notification_date = self.calculate_notification_date(now, selected_day)
            
            pair_data['organizer'] = organizer_id
            pair_data['next_date'] = selected_day
            pair_data['theme'] = theme
            pair_data['notification_sent'] = False
            pair_data['notification_date'] = notification_date.isoformat()
            
            self.set_pair_data(pair_id, pair_data)
            
            logger.info(f"✅ Для пары {pair_id} запланировано: {selected_day} число, организатор: {organizer_id}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка планирования для пары {pair_data['id']}: {e}")

    def calculate_notification_date(self, current_date, selected_day):
        """Рассчитывает дату уведомления"""
        meeting_date = datetime(current_date.year, current_date.month, selected_day)
        
        notification_date = meeting_date - timedelta(days=3)
        notification_date = notification_date.replace(hour=8, minute=59, second=0, microsecond=0)
        
        return notification_date

    async def send_scheduled_notification(self, context: ContextTypes.DEFAULT_TYPE, pair_id):
        """Отправляет уведомление организатору в запланированное время"""
        try:
            pair_data = self.get_pair_data(pair_id)
            if not pair_data:
                logger.error(f"❌ Пара {pair_id} не найдена")
                return
            
            if pair_data.get('notification_sent'):
                logger.info(f"ℹ️ Уведомление для пары {pair_id} уже отправлено")
                return
            
            organizer_id = pair_data['organizer']
            selected_day = pair_data['next_date']
            theme = pair_data['theme']
            
            current_month = datetime.now().month
            month_name = month_names[current_month]

            days_difference = selected_day - datetime.now().day
            
            await context.bot.send_message(
                chat_id=organizer_id,
                text=f"🎉 *В этом месяце твоя очередь организовывать свидание!*\n\n"
                     f"📅 Свидание должно состояться {selected_day} {month_name}\n"
                     f"🎨 Тема свидания: {theme}\n"
                     f"⏰ У тебя есть {days_difference} дня на организацию!",
                parse_mode='markdown'
            )
            
            pair_data['notification_sent'] = True
            self.set_pair_data(pair_id, pair_data)
            
            logger.info(f"✅ Уведомление отправлено организатору {organizer_id} (осталось дней: {days_difference})")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления для пары {pair_id}: {e}")

    async def check_and_send_pending_notifications(self, context: ContextTypes.DEFAULT_TYPE):
        """Проверяет и отправляет уведомления, которые должны были быть отправлены"""
        try:            
            now = datetime.now()
            logger.info(f"🔍 Проверка ожидающих уведомлений: {now}")
            
            pairs = self.get_all_pairs()
            sent_count = 0
            
            for pair_data in pairs:
                if not pair_data.get('notification_sent'):
                    notification_date_str = pair_data.get('notification_date')
                    if notification_date_str:
                        notification_date = datetime.fromisoformat(notification_date_str)
                        
                        if now >= notification_date:
                            await self.send_scheduled_notification(context, pair_data['id'])
                            sent_count += 1

                        logger.info(f"now: {now}")
                        logger.info(f"notification_date: {notification_date}")
            
            logger.info(f"📨 Отправлено отложенных уведомлений: {sent_count}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки уведомлений: {e}")

    def run(self):
        """Синхронный запуск бота"""
        application = Application.builder().token(self.token).build()
        
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("partner", self.partner_command))
        application.add_handler(CommandHandler("status", self.status_command))
        application.add_handler(CommandHandler("help", self.help_command))
        application.add_handler(CommandHandler("cancel", self.cancel_command))

        application.add_handler(CallbackQueryHandler(self.button_handler))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        if hasattr(application, 'job_queue') and application.job_queue:
            
            application.job_queue.run_daily(
                self.keep_redis_awake,
                time=time(hour=5, minute=57),
                name="ping_daily"
            )
            
            application.job_queue.run_monthly(
                self.monthly_planning,
                when=time(hour=5, minute=58),
                day=1,
                name="monthly_planning"
            )
            
            application.job_queue.run_daily(
                self.check_and_send_pending_notifications,
                time=time(hour=6, minute=0),
                name="daily_notification_check"
            )
            
            logger.info("✅ JobQueue настроен для планирования свиданий")
        else:
            logger.error("❌ JobQueue не доступен! Планирование не будет работать")

        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )

if __name__ == "__main__":
    bot = MonthlyMeetBot()
    bot.run()

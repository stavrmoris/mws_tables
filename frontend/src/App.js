import React, { useState, useEffect, useRef, useMemo} from 'react';
import { Table, Button, Input, Select, Card, Space, Modal, message, DatePicker } from 'antd';
import { LineChart, Line, BarChart, Bar, PieChart, Pie, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { FilterOutlined, MessageOutlined, DownloadOutlined } from '@ant-design/icons';
import './App.css';

const { Option } = Select;
const { Search } = Input;
const { RangePicker } = DatePicker;

const App = () => {
  const [data, setData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [chatVisible, setChatVisible] = useState(false);
  const [filters, setFilters] = useState({
    search: '',
    contentType: 'all',
    dateRange: 'all',
    dateRangeCustom: null,
    chartMetric: 'all' // Добавляем фильтр для метрик графика
  });

  const BACKEND_CONFIG = {
    apiUrl: 'http://localhost:8000',
    endpoints: {
      data: '/api/data',
      stats: '/api/stats/overview', 
      sentiment: '/api/analytics/sentiment',
      topContent: '/api/top/content',
      sources: '/api/sources/performance',
      chat: '/chat', 
      export: '/api/export/csv',
    }
  };

  const CHATBOT_CONFIG = {
    apiUrl: 'http://localhost:8000/chat'
  };

  // Загрузка данных из бэкенда
const fetchData = async () => {
  setLoading(true);
  try {
    const response = await fetch(`${BACKEND_CONFIG.apiUrl}${BACKEND_CONFIG.endpoints.data}`);
    
    if (!response.ok) throw new Error('Ошибка бэкенда: ' + response.status);
    
    const result = await response.json();
    console.log('Данные от бэкенда:', result);
    
    const formattedData = result.data.map(item => {
      let dateValue = item.fields?.Дата || '2024-01-01';

      // Преобразуем дату в формат "день-месяц-год"
      if (typeof dateValue === 'number' && dateValue > 1000000000000) {
        const dateObj = new Date(dateValue);
        const day = String(dateObj.getDate()).padStart(2, '0');
        const month = String(dateObj.getMonth() + 1).padStart(2, '0');
        const year = dateObj.getFullYear();
        dateValue = `${day}-${month}-${year}`;
      }
      else if (dateValue.includes('-')) {
        const [year, month, day] = dateValue.split('-');
        dateValue = `${day}-${month}-${year}`;
      }

      return {
        key: item.id || `record_${Date.now()}_${Math.random()}`,
        id: item.id,
        title: item.fields?.Название || 'Без названия',
        type: item.fields?.Источник?.toLowerCase() || 'unknown',
        date: dateValue,
        views: item.fields?.Просмотры || 0,
        likes: item.fields?.Лайки || 0,
        reposts: item.fields?.Репосты || 0, 
        engagement: item.fields?.engagement || 0,
        sentiment: item.fields?.Тональность || 'Neutral'
      };
    });

    // Проверяем, есть ли новые данные
    const previousDataCount = data.length;
    const newDataCount = formattedData.length;
    
    setData(formattedData);
    setFilteredData(formattedData);
    
    // Показываем уведомление в зависимости от результата
    if (newDataCount > previousDataCount) {
      const newRecordsCount = newDataCount - previousDataCount;
      message.success({
        content: `Данные успешно обновлены! Добавлено ${newRecordsCount} новых записей`,
        duration: 3,
        style: {
          marginTop: '50px',
        }
      });
    } else if (newDataCount === previousDataCount && previousDataCount > 0) {
      message.info({
        content: ' Нет новых данных',
        duration: 3,
        style: {
          marginTop: '50px',
        }
      });
    } else if (previousDataCount === 0 && newDataCount > 0) {
      message.success({
        content: `✅ Загружено ${newDataCount} записей`,
        duration: 3,
        style: {
          marginTop: '50px',
        }
      });
    }
    
  } catch (error) {
    console.error('Ошибка:', error);
    message.error({
      content: '❌ Ошибка загрузки данных от бэкенда',
      duration: 3,
      style: {
        marginTop: '50px',
      }
    });
    setData([]);
    setFilteredData([]);
  } finally {
    setLoading(false);
  }
};


const handleExport = async () => {
  try {
    const response = await fetch(`${BACKEND_CONFIG.apiUrl}${BACKEND_CONFIG.endpoints.export}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) throw new Error('Ошибка экспорта');

    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.style.display = 'none';
    a.href = url;
    a.download = `content-registry-${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);
    
    message.success('Данные успешно экспортированы в CSV');
  } catch (error) {
    console.error('Ошибка экспорта:', error);
    message.error('Ошибка при экспорте данных');
  }
};

  useEffect(() => {
    fetchData();
  }, []);

  // Применение фильтров
  useEffect(() => {
    let filtered = [...data];
    
    // Поиск по названию
    if (filters.search) {
      filtered = filtered.filter(item => 
        item.title.toLowerCase().includes(filters.search.toLowerCase())
      );
    }
    
    // Фильтр по типу контента (источнику)
    if (filters.contentType !== 'all') {
      filtered = filtered.filter(item => item.type === filters.contentType);
    }
    
    // Фильтр по кастомному диапазону дат
    if (filters.dateRangeCustom && filters.dateRangeCustom[0] && filters.dateRangeCustom[1]) {
      filtered = filtered.filter(item => {
        const itemDate = new Date(item.date.split('-').reverse().join('-'));
        const startDate = filters.dateRangeCustom[0].toDate();
        const endDate = filters.dateRangeCustom[1].toDate();
        return itemDate >= startDate && itemDate <= endDate;
      });
    }
    
    setFilteredData(filtered);
  }, [filters, data]);

  // Колонки таблицы
const columns = [
  {
    title: 'Название',
    dataIndex: 'title',
    key: 'title',
    width: 200,
    render: (text) => <strong>{text}</strong>,
  },
  {
    title: 'Источник',
    dataIndex: 'type',
    key: 'type',
    width: 100,
    render: (type) => {
      const sourceMap = {
        'telegram': 'Telegram',
        'vk': 'VK', 
        'youtube': 'YouTube',
        'rutube': 'Rutube',
        'habr': 'Habr'
      };
      return sourceMap[type] || type;
    }
  },
  {
    title: 'Дата',
    dataIndex: 'date',
    key: 'date',
    width: 120,
    sorter: (a, b) => new Date(a.date.split('-').reverse().join('-')) - new Date(b.date.split('-').reverse().join('-')),
    render: (date) => {
      if (!date) return '-';
      return date;
    }
  },
  {
    title: 'Просмотры',
    dataIndex: 'views',
    key: 'views',
    width: 100,
    sorter: (a, b) => a.views - b.views,
    render: (views) => new Intl.NumberFormat('ru-RU').format(views),
  },
  {
    title: 'Лайки',
    dataIndex: 'likes',
    key: 'likes',
    width: 80,
    sorter: (a, b) => a.likes - b.likes,
    render: (likes) => new Intl.NumberFormat('ru-RU').format(likes),
  },
  {
    title: 'Репосты',
    dataIndex: 'reposts',
    key: 'reposts',
    width: 80,
    sorter: (a, b) => a.reposts - b.reposts,
    render: (reposts) => new Intl.NumberFormat('ru-RU').format(reposts),
  },
  {
  title: 'Тональность',
  dataIndex: 'sentiment',
  key: 'sentiment',
  width: 120,
  sorter: (a, b) => {
    const sentimentOrder = {
      'Positive': 1,
      'Neutral': 2, 
      'Negative': 3
    };
    const orderA = sentimentOrder[a.sentiment] || 4;
    const orderB = sentimentOrder[b.sentiment] || 4;
    return orderA - orderB;
  },
  render: (sentiment) => {
    const sentimentMap = {
      'Positive': 'Позитивная',
      'Negative': 'Негативная', 
      'Neutral': 'Нейтральная'
    };
    
    const colorMap = {
      'Positive': '#52c41a',
      'Negative': '#ff4d4f', 
      'Neutral': '#faad14'
    };
    
    const displayText = sentimentMap[sentiment] || sentiment;
    
    return (
      <span 
        style={{
          color: colorMap[sentiment],
          fontWeight: '600',
          padding: '4px 12px',
          borderRadius: '20px',
          backgroundColor: `${colorMap[sentiment]}15`,
          border: `1px solid ${colorMap[sentiment]}30`
        }}
      >
        {displayText}
      </span>
    );
  }
  },
];

  // Данные для графиков с агрегацией по датам и учетом диапазона
const chartData = useMemo(() => {
  let dataToUse = filteredData;
  
  // Если выбран кастомный диапазон дат, дополнительно фильтруем данные для графика
  if (filters.dateRangeCustom && filters.dateRangeCustom[0] && filters.dateRangeCustom[1]) {
    dataToUse = filteredData.filter(item => {
      const itemDate = new Date(item.date.split('-').reverse().join('-'));
      const startDate = filters.dateRangeCustom[0].toDate();
      const endDate = filters.dateRangeCustom[1].toDate();
      return itemDate >= startDate && itemDate <= endDate;
    });
  }
  
  if (filters.contentType === 'all') {
    // Агрегируем данные по датам
    const aggregatedData = dataToUse.reduce((acc, item) => {
      if (!acc[item.date]) {
        acc[item.date] = {
          date: item.date,
          views: 0,
          likes: 0,
          reposts: 0,
          count: 0
        };
      }
      acc[item.date].views += item.views;
      acc[item.date].likes += item.likes;
      acc[item.date].reposts += item.reposts;
      acc[item.date].count += 1;
      return acc;
    }, {});

    return Object.values(aggregatedData).sort((a, b) => {
      const dateA = new Date(a.date.split('-').reverse().join('-'));
      const dateB = new Date(b.date.split('-').reverse().join('-'));
      return dateA - dateB;
    });
  } else {
    // Обычные данные без агрегации
    return dataToUse.map(item => ({
      date: item.date,
      title: item.title,
      views: item.views,
      likes: item.likes,
      reposts: item.reposts,
      engagement: item.engagement * 100,
    })).sort((a, b) => {
      const dateA = new Date(a.date.split('-').reverse().join('-'));
      const dateB = new Date(b.date.split('-').reverse().join('-'));
      return dateA - dateB;
    });
  }
}, [filteredData, filters.contentType, filters.dateRangeCustom]);

  return (
    <div className="app">
      {/* Заголовок и фильтры */}
      <Card className="filters-card">
        <div className="filters-header">
          <h1>📊 Умный реестр контента</h1>
          <div className="chat-bot-section">
            <Button 
              type="primary" 
              icon={<MessageOutlined />}
              onClick={() => setChatVisible(true)}
              className="chat-button"
            >
              Чат-бот аналитики
            </Button>
            <div className="telegram-bot-link">
              <a 
                href="https://t.me/mw_table_bot" 
                target="_blank" 
                rel="noopener noreferrer"
                style={{
                  fontSize: '12px',
                  color: '#1890ff',
                  textDecoration: 'none',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '4px',
                  marginTop: '4px'
                }}
              >
                <MessageOutlined style={{ fontSize: '10px' }} />
                Умный ассистент в Telegram
              </a>
            </div>
          </div>
        </div>
        
        <Space size="middle" className="filters-space">
        <Search
          placeholder="Поиск по названию..."
          value={filters.search}
          onChange={(e) => setFilters({...filters, search: e.target.value})}
          style={{ width: 300 }}
        />
        
        <Select
          value={filters.contentType}
          onChange={(value) => setFilters({...filters, contentType: value})}
          style={{ width: 150 }}
        >
          <Option value="all">Все источники</Option>
          <Option value="telegram">Telegram</Option>
          <Option value="youtube">YouTube</Option>
          <Option value="vk">VK</Option>
          <Option value="rutube">Rutube</Option>
          <Option value="habr">Habr</Option>
        </Select>
        
        <Select
          value={filters.chartMetric}
          onChange={(value) => setFilters({...filters, chartMetric: value})}
          style={{ width: 180 }}
        >
          <Option value="all">Общая статистика</Option>
          <Option value="views">Просмотры</Option>
          <Option value="likes">Лайки</Option>
          <Option value="reposts">Репосты</Option>
        </Select>
        
        <RangePicker
          value={filters.dateRangeCustom}
          onChange={(dates) => setFilters({...filters, dateRangeCustom: dates})}
          placeholder={['Начальная дата', 'Конечная дата']}
          style={{ width: 280 }}
          format="DD-MM-YYYY"
        />
        
        {filters.dateRangeCustom && (
          <Button 
            onClick={() => setFilters({...filters, dateRangeCustom: null})}
            style={{ 
              background: 'transparent', 
              border: '1px solid #d9d9d9',
              color: '#666'
            }}
          >
            Сбросить даты
          </Button>
        )}
        
        <Button 
          icon={<FilterOutlined />}
          onClick={fetchData}
          loading={loading}
        >
          Обновить данные
        </Button>
      </Space>
      </Card>

      {/* Графики */}
<div className="charts-section">
  <Card 
    title={
      filters.chartMetric === 'all' ? 'Общая статистика' :
      filters.chartMetric === 'views' ? 'Просмотры' :
      filters.chartMetric === 'likes' ? 'Лайки' :
      filters.chartMetric === 'reposts' ? 'Репосты' : 'Статистика'
    } 
    className="chart-card"
  >
  <ResponsiveContainer width="100%" height={400}>
    <LineChart data={chartData}>
      <CartesianGrid strokeDasharray="3 3" />
      <XAxis 
        dataKey="date" 
        tickFormatter={(date) => date}
      />
      <YAxis />
      <Tooltip 
        formatter={(value, name) => {
          const formatter = new Intl.NumberFormat('ru-RU');
          return [formatter.format(value), name];
        }}
        labelFormatter={(date, items) => {
          if (items && items[0]) {
            if (filters.contentType === 'all') {
              // Для агрегированных данных показываем суммарную статистику
              const payload = items[0].payload;
              return (
                <div>
                  <div style={{ fontWeight: 'bold', marginBottom: '8px', fontSize: '14px' }}>
                    📅 {date}
                  </div>
                  <div style={{ fontSize: '12px', color: '#666' }}>
                    Всего публикаций: {payload.count || 1}
                  </div>
                </div>
              );
            } else {
              // Для отдельных источников показываем название
              return (
                <div>
                  <div style={{ fontWeight: 'bold', marginBottom: '4px' }}>📅 {date}</div>
                  <div style={{ fontStyle: 'italic', color: '#666' }}>
                    "{items[0].payload.title}"
                  </div>
                </div>
              );
            }
          }
          return `Дата: ${date}`;
        }}
      />
      <Legend />
      {/* Показываем линии в зависимости от выбранной метрики */}
      {(filters.chartMetric === 'all' || filters.chartMetric === 'views') && (
        <Line 
          type="monotone" 
          dataKey="views" 
          stroke="#8884d8" 
          name="Просмотры" 
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 6 }}
        />
      )}
      {(filters.chartMetric === 'all' || filters.chartMetric === 'likes') && (
        <Line 
          type="monotone" 
          dataKey="likes" 
          stroke="#82ca9d" 
          name="Лайки" 
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 6 }}
        />
      )}
      {(filters.chartMetric === 'all' || filters.chartMetric === 'reposts') && (
        <Line 
          type="monotone" 
          dataKey="reposts" 
          stroke="#ff7300" 
          name="Репосты" 
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 6 }}
        />
      )}
    </LineChart>
  </ResponsiveContainer>
  </Card>
</div>

{/* Основная таблица */}
<Card 
  title={`Реестр контента (${filteredData.length} записей)`}
  extra={
    // Показываем кнопку экспорта только когда нет активных фильтров
    (filters.contentType === 'all' && !filters.dateRangeCustom) ? (
      <Button 
        type="primary" 
        onClick={handleExport}
        icon={<DownloadOutlined />}
        className="export-button"
      >
        Экспорт CSV
      </Button>
    ) : (
      <div style={{ 
        padding: '8px 16px', 
        background: '#f5f5f5', 
        borderRadius: '8px',
        color: '#999',
        fontSize: '12px',
        fontStyle: 'italic'
      }}>
        Экспорт доступен только для всей таблицы
      </div>
    )
  }
>
  <Table
    columns={columns}
    dataSource={filteredData}
    loading={loading}
    pagination={{ pageSize: 10 }}
    scroll={{ x: 800 }}
  />
</Card>

      {/* Окно чат-бота */}
      <ChatBotWindow
        visible={chatVisible}
        onClose={() => setChatVisible(false)}
        apiConfig={CHATBOT_CONFIG}
        currentData={filteredData}
      />
    </div>
  );
};

// Компонент окна чат-бота
const ChatBotWindow = ({ visible, onClose, apiConfig, currentData }) => {
  const [messages, setMessages] = useState([]);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const sendMessage = async () => {
    if (!inputValue.trim()) return;

    const userMessage = {
      id: Date.now(),
      type: 'user',
      text: inputValue,
      timestamp: new Date()
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setLoading(true);

    try {
      const response = await fetch(apiConfig.apiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          question: inputValue 
        })
      });

      if (!response.ok) throw new Error('Ошибка сервера');

      const result = await response.json();
      
      const botMessage = {
        id: Date.now() + 1,
        type: 'bot',
        text: result.answer || 'Извините, не могу обработать запрос',
        timestamp: new Date()
      };

      setMessages(prev => [...prev, botMessage]);
    } catch (error) {
      const errorMessage = {
        id: Date.now() + 1,
        type: 'bot',
        text: 'Ошибка соединения с сервером. Проверьте настройки API.',
        timestamp: new Date(),
        isError: true
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      title="🤖 Чат-бот аналитики контента"
      open={visible}
      onCancel={onClose}
      footer={null}
      width={600}
      style={{ top: 20 }}
    >
      <div className="chat-window">
        <div className="chat-messages">
          {messages.length === 0 && (
            <div className="welcome-message">
              <p>Задайте вопросы о вашем контенте на естественном языке!</p>
              <p>Примеры:</p>
              <ul>
                <li>"Какой контент был самым популярным?"</li>
                <li>"Покажи статистику за последнюю неделю"</li>
                <li>"Какие посты получили больше всего комментариев?"</li>
              </ul>
            </div>
          )}
          
          {messages.map(message => (
            <div key={message.id} className={`message ${message.type} ${message.isError ? 'error' : ''}`}>
              <div className="message-content">
                {message.text}
              </div>
              <div className="message-time">
                {message.timestamp.toLocaleTimeString()}
              </div>
            </div>
          ))}
          
          {loading && (
            <div className="message bot">
              <div className="message-content typing">
                Бот печатает...
              </div>
            </div>
          )}
          
          <div ref={messagesEndRef} />
        </div>
        
        <div className="chat-input">
          <Input
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onPressEnter={sendMessage}
            placeholder="Задайте вопрос о вашем контенте..."
            disabled={loading}
          />
          <Button 
            type="primary" 
            onClick={sendMessage}
            loading={loading}
          >
            Отправить
          </Button>
        </div>
      </div>
    </Modal>
  );
};

export default App;
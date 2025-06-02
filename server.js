const express = require('express');
const { google } = require('googleapis');
const { BigQuery } = require('@google-cloud/bigquery'); // Thêm dòng này
const cors = require('cors');
const NodeCache = require('node-cache');
const cache = new NodeCache({ stdTTL: 3600 }); // Cache 1 giờ

const app = express();
app.use(cors({ origin: ['https://reawake-web.onrender.com', 'http://localhost:3000'] })); // Cho phép tất cả các nguồn
app.use(express.json());

const SPREADSHEET_ID = '1BUGQrNXqfWftJQlzzMj6umJz7yGeNAkGJnzji3zY2sc';
const credentials = require('./credentials.json');

async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version: 'v4', auth });
}

// Thêm hàm khởi tạo BigQuery client
async function getBigQueryClient() {
  const bigquery = new BigQuery({
    projectId: 'kamereo-351910',
    keyFilename: './credentials.json' // Sử dụng cùng file credentials
  });
  return bigquery;
}

// Hàm phân tích và định dạng ngày tháng
function parseAndFormatDate(dateStr, format) {
  if (!dateStr) return null;

  // Xử lý đối tượng BigQueryDate
  if (dateStr && typeof dateStr === 'object' && dateStr.value) {
    dateStr = dateStr.value;
  }

  // Chuyển đổi sang string nếu không phải string
  const dateString = String(dateStr);

  // Xác định định dạng và parse
  const formats = [
    { pattern: /^(\d{2})\/(\d{2})\/(\d{4})$/, parse: (match) => new Date(match[3], match[2] - 1, match[1]) },
    { pattern: /^(\d{4})-(\d{2})-(\d{2})$/, parse: (match) => new Date(match[1], match[2] - 1, match[3]) }
  ];

  for (const { pattern, parse } of formats) {
    const match = dateString.match(pattern);
    if (match) {
      const date = parse(match);
      if (!isNaN(date)) {
        // Định dạng lại theo yêu cầu
        if (format === 'DD/MM/YYYY') {
          return `${String(date.getDate()).padStart(2, '0')}/${String(date.getMonth() + 1).padStart(2, '0')}/${date.getFullYear()}`;
        } else if (format === 'YYYY-MM-DD') {
          return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
        }
        return date;
      }
    }
  }

  console.error(`Cannot parse date: ${dateString}`);
  return null;
}

function calculateDaysSinceLastOrder(lastOrderDate) {
  const lastOrder = parseAndFormatDate(lastOrderDate, 'YYYY-MM-DD');
  const currentDate = new Date();
  const diffTime = Math.abs(currentDate - lastOrder);
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
}

async function checkStoreAccess(sheets, email, storeId) {
  const picCode = email.split('@')[0];
  const decenResponse = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'Ex Decentralization!A:F'
  });
  const decenRows = decenResponse.data.values || [];
  const userDecen = decenRows.find(row => row[0] === picCode);
  if (!userDecen) return false;

  const role = userDecen[2] || 'Member';
  const subteam = userDecen[1] || 'N/A';
  const region = userDecen[3] || 'N/A';
  const team = userDecen[4] || 'N/A';
  const concat = userDecen[5] || 'N/A';

  // Thay đổi: Sử dụng BigQuery thay vì Sheets
  const storeRows = await getStoreInfoFromBigQuery();
  const store = storeRows.slice(1).find(row => row[0] === storeId);
  if (!store) return false;

  const storePIC = store[5]; // finalCurrentPIC

  if (role === 'Member') {
    return storePIC === picCode;
  } else if (role === 'Leader') {
    const subteamPICs = decenRows.filter(row => row[1] === subteam && row[0]).map(row => row[0]);
    return subteamPICs.includes(storePIC);
  } else if (role === 'Manager') {
    if (region === 'ALL') {
      return true;
    } else if (region === 'HCM') {
      const concatPICs = decenRows.filter(row => row[5] === concat).map(row => row[0]);
      return concatPICs.includes(storePIC);
    } else if (region === 'HN') {
      if (team === 'ALL') {
        const hnPICs = decenRows.filter(row => row[3] === 'HN').map(row => row[0]);
        return hnPICs.includes(storePIC);
      } else {
        const concatPICs = decenRows.filter(row => row[5] === concat).map(row => row[0]);
        return concatPICs.includes(storePIC);
      }
    }
  }
  return false; // Thêm return mặc định
}

app.post('/login', async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Email is required' });

  try {
    const sheets = await getSheetsClient();
    const cacheKey = `login_${email}`;
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Authentication!A:K'
    });
    const authRows = response.data.values || [];
    let isActive = false;

    for (let i = 1; i < authRows.length; i++) {
      if (authRows[i][2] === email && authRows[i][10] === 'Active') {
        isActive = true;
        break;
      }
    }

    const result = isActive ? { success: true } : { error: 'Email not found or account not active' };
    cache.set(cacheKey, result);
    res.json(result);
  } catch (error) {
    console.error('Error checking email:', error);
    res.status(500).json({ error: 'Failed to check email', details: error.message });
  }
});

app.post('/manual-login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) {
    return res.status(400).json({ success: false, error: 'Email and password are required' });
  }

  try {
    const sheets = await getSheetsClient();
    
    const authResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Authentication!A:N'  // Mở rộng range để bao gồm cột PASSWORD (N)
    });
    
    const authRows = authResponse.data.values || [];
    let userFound = false;
    let isActive = false;
    let correctPassword = false;
    let picInfo = null;
    
    for (let i = 1; i < authRows.length; i++) {
      const row = authRows[i];
      if (row[2] === email) {  // Cột C chứa email
        userFound = true;
        isActive = row[10] === 'Active';  // Cột K chứa STATUS
        correctPassword = row[13] === password;  // Cột N chứa PASSWORD
        
        if (isActive && correctPassword) {
          picInfo = {
            fullName: row[3] || 'N/A',  // Cột D chứa NAME
            email: row[2] || 'N/A'      // Cột C chứa EMAIL
          };
          break;
        }
      }
    }
    
    if (!userFound) {
      return res.status(404).json({ success: false, error: 'Account not found' });
    }
    
    if (!isActive) {
      return res.status(403).json({ success: false, error: 'Account is not active' });
    }
    
    if (!correctPassword) {
      return res.status(401).json({ success: false, error: 'Incorrect password' });
    }
    
    // Xử lý tiếp tục như endpoint login thông thường
    // Lấy thông tin về quyền truy cập vào các store
    // Tương tự như trong endpoint app.post('/login', ...)
    
    // Nếu mọi thứ OK
    return res.json({ success: true, picInfo });
  } catch (error) {
    console.error('Error validating manual login:', error);
    res.status(500).json({ success: false, error: 'Failed to validate credentials', details: error.message });
  }
});

app.post('/home', async (req, res) => {
  const { email } = req.body;
  const force = req.query.force === 'true';
  const cacheKey = `home_${email}`;
  
  // Nếu force=true thì bỏ qua cache
  if (!force) {
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);
  }
  
  try {
    const sheets = await getSheetsClient();

    const picCode = email.split('@')[0];
    const authResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Authentication!A:M'
    });
    const authRows = authResponse.data.values || [];
    let picInfo = null;

    for (let i = 1; i < authRows.length; i++) {
      if (authRows[i][2] === email) {
        picInfo = {
          fullName: authRows[i][1] || '',
          email: authRows[i][2] || '',
          status: authRows[i][10] || '',
          team: authRows[i][4] || 'N/A'
        };
        break;
      }
    }

    if (!picInfo) return res.status(404).json({ error: 'Email not found in Authentication' });

    const decenResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Decentralization!A:F'
    });
    const decenRows = decenResponse.data.values || [];
    const userDecen = decenRows.find(row => row[0] === picCode);
    if (userDecen) {
      picInfo.subteam = userDecen[1] || 'N/A';
      picInfo.role = userDecen[2] || 'Member';
      picInfo.region = userDecen[3] || 'N/A';
      picInfo.team = userDecen[4] || 'N/A';
      picInfo.concat = userDecen[5] || 'N/A';
    } else {
      picInfo.subteam = 'N/A';
      picInfo.role = 'Member';
      picInfo.region = 'N/A';
      picInfo.team = 'N/A';
      picInfo.concat = 'N/A';
    }

    // Thay thế gọi Ex Store_info bằng BigQuery
    const storeRows = await getStoreInfoFromBigQuery();
    const dataRows = storeRows.slice(1); // Bỏ qua dòng header
    let stores = [];

    if (picInfo.role === 'Member') {
      stores = dataRows.filter(row => row[5] === picCode).map(row => ({
        storeId: row[0] || '',
        storeName: row[1] || '',
        buyerId: row[2] || '',
        fullAddress: row[9] || '',
        lastOrderDate: row[11] || '',
        finalCurrentPIC: row[5] || '',
        statusChurnThisMonth: row[12] || ''
      }));
    } else if (picInfo.role === 'Leader') {
      const subteamPICs = decenRows.filter(row => row[1] === picInfo.subteam && row[0]).map(row => row[0]);
      stores = dataRows.filter(row => row[5] && subteamPICs.includes(row[5])).map(row => ({
        storeId: row[0] || '',
        storeName: row[1] || '',
        buyerId: row[2] || '',
        fullAddress: row[9] || '',
        lastOrderDate: row[11] || '',
        finalCurrentPIC: row[5] || '',
        statusChurnThisMonth: row[12] || ''
      }));
    } else if (picInfo.role === 'Manager') {
      if (picInfo.region === 'ALL') {
        stores = dataRows.map(row => ({
          storeId: row[0] || '',
          storeName: row[1] || '',
          buyerId: row[2] || '',
          fullAddress: row[9] || '',
          lastOrderDate: row[11] || '',
          finalCurrentPIC: row[5] || '',
          statusChurnThisMonth: row[12] || ''
        }));
      } else if (picInfo.region === 'HCM') {
        const concatPICs = decenRows.filter(row => row[5] === picInfo.concat).map(row => row[0]);
        stores = dataRows.filter(row => row[5] && concatPICs.includes(row[5])).map(row => ({
          storeId: row[0] || '',
          storeName: row[1] || '',
          buyerId: row[2] || '',
          fullAddress: row[9] || '',
          lastOrderDate: row[11] || '',
          finalCurrentPIC: row[5] || '',
          statusChurnThisMonth: row[12] || ''
        }));
      } else if (picInfo.region === 'HN') {
        if (picInfo.team === 'ALL') {
          const hnPICs = decenRows.filter(row => row[3] === 'HN').map(row => row[0]);
          stores = dataRows.filter(row => row[5] && hnPICs.includes(row[5])).map(row => ({
            storeId: row[0] || '',
            storeName: row[1] || '',
            buyerId: row[2] || '',
            fullAddress: row[9] || '',
            lastOrderDate: row[11] || '',
            finalCurrentPIC: row[5] || '',
            statusChurnThisMonth: row[12] || ''
          }));
        } else {
          const concatPICs = decenRows.filter(row => row[5] === picInfo.concat).map(row => row[0]);
          stores = dataRows.filter(row => row[5] && concatPICs.includes(row[5])).map(row => ({
            storeId: row[0] || '',
            storeName: row[1] || '',
            buyerId: row[2] || '',
            fullAddress: row[9] || '',
            lastOrderDate: row[11] || '',
            finalCurrentPIC: row[5] || '',
            statusChurnThisMonth: row[12] || ''
          }));
        }
      }
    }

    stores.sort((a, b) => {
      const dateA = parseAndFormatDate(a.lastOrderDate, 'YYYY-MM-DD');
      const dateB = parseAndFormatDate(b.lastOrderDate, 'YYYY-MM-DD');
      return dateB - dateA;
    });

    const result = { picInfo, stores };
    cache.set(cacheKey, result);
    res.json(result);
  } catch (error) {
    console.error('Error fetching data:', error);
    res.status(500).json({ error: 'Failed to fetch data', details: error.message });
  }
});

app.post('/submit', async (req, res) => {
  const { email, storeId, storeName, action, contactDate, PIC, subteam, typeOfContact, note, whyNotReawaken, churnMonthLastOrderDate, activeMonth, linkHubspot } = req.body;
  const type = req.query.type;
  if (!email || !storeId || !contactDate || !typeOfContact || !action || !type) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  let tableName = '';
  try {
    const sheets = await getSheetsClient(); // Vẫn cần để kiểm tra quyền truy cập

    // Kiểm tra quyền truy cập store
    const hasAccess = await checkStoreAccess(sheets, email, storeId);
    if (!hasAccess) {
      return res.status(403).json({ error: 'You do not have permission to record actions for this store' });
    }

    const bigquery = await getBigQueryClient();
    
    // Chuyển đổi định dạng ngày từ dd/mm/yyyy sang yyyy-mm-dd cho BigQuery
    let formattedContactDate = contactDate;
    if (contactDate && contactDate.match(/^(\d{2})\/(\d{2})\/(\d{4})$/)) {
      const [_, day, month, year] = contactDate.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
      formattedContactDate = `${year}-${month}-${day}`;
    }
    
    // Ghi dữ liệu cho Churn Database
    if (type === 'Churn Database') {
      tableName = 'kamereo-351910.Sales_team_information_create_by_Phong_BOS.Churn_Database_table';
      
      // Chuyển đổi định dạng ngày cho churnMonthLastOrderDate
      let formattedChurnMonth = churnMonthLastOrderDate;
      if (churnMonthLastOrderDate && churnMonthLastOrderDate.match(/^(\d{2})\/(\d{2})\/(\d{4})$/)) {
        const [_, day, month, year] = churnMonthLastOrderDate.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
        formattedChurnMonth = `${year}-${month}-${day}`;
      }
      
      // Chuẩn bị dữ liệu để insert vào BigQuery
      const row = {
        Store_ID: storeId,
        Store_Name: storeName || '',
        Contact_Date: formattedContactDate,
        PIC: PIC,
        Subteam: subteam,
        Type_of_Contact: typeOfContact,
        Action: action,
        Note: note || '',
        Why_not_reawaken: whyNotReawaken || '',
        Churn_Month: formattedChurnMonth,
        Link_Hubspot: linkHubspot || ''
      };
      
      await bigquery.dataset('Sales_team_information_create_by_Phong_BOS').table('Churn_Database_table').insert([row]);
      
    } 
    // Ghi dữ liệu cho Active Database
    else {
      tableName = 'kamereo-351910.Sales_team_information_create_by_Phong_BOS.Active_Database_table';
      
      // Xử lý activeMonth, giữ định dạng mm/yyyy cho BigQuery
      let formattedActiveMonth = activeMonth;
      // Nếu muốn lưu dưới dạng yyyy-mm-01 thì mở comment phần này
      /* 
      if (activeMonth && activeMonth.match(/^(\d{2})\/(\d{4})$/)) {
        const [_, month, year] = activeMonth.match(/^(\d{2})\/(\d{4})$/);
        formattedActiveMonth = `${year}-${month}-01`; // Set ngày là 01
      }
      */
      
      // Chuẩn bị dữ liệu để insert vào BigQuery
      const row = {
        Store_ID: storeId,
        Store_Name: storeName || '',
        Contact_Date: formattedContactDate,
        PIC: PIC,
        Subteam: subteam,
        Type_of_Contact: typeOfContact,
        Action: action,
        Note: note || '',
        Active_Month: formattedActiveMonth,
        Link_Hubspot: linkHubspot || ''
      };
      
      await bigquery.dataset('Sales_team_information_create_by_Phong_BOS').table('Active_Database_table').insert([row]);
      
    }

    // Clear cache for related endpoints
    cache.del(`home_${email}`);
    cache.del(`progress_${email}`);
    res.json({ success: true });
  } catch (error) {
    console.error(`Error writing data to ${tableName}:`, error);
    
    // Cung cấp thông tin lỗi chi tiết hơn
    let errorMessage = error.message;
    if (error.errors && error.errors.length > 0) {
      errorMessage = error.errors.map(err => err.message).join('; ');
    }
    
    res.status(error.code === 'ECONNRESET' ? 503 : 500).json({ 
      error: `Failed to write data to ${tableName}`,
      details: errorMessage 
    });
  }
});

app.post('/progress', async (req, res) => {
  const { email } = req.body;
  const force = req.query.force === 'true';
  const cacheKey = `progress_${email}`;
  
  // Nếu force=true thì bỏ qua cache
  if (!force) {
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);
  }
  
  try {
    // Lấy dữ liệu từ BigQuery thay vì Sheets
    const churnHistoryRows = await getChurnHistoryFromBigQuery();
    const activeHistoryRows = await getActiveHistoryFromBigQuery();
    const storeRows = await getStoreInfoFromBigQuery();
    const churnDatabaseRows = await getChurnDatabaseFromBigQuery();
    const activeDatabaseRows = await getActiveDatabaseFromBigQuery();
    
    const sheets = await getSheetsClient(); // Vẫn cần cho các sheet khác như Ex Decentralization
    
    // Phần còn lại của code giữ nguyên như đã có
    const churnHistoryByStore = {};
    for (let i = 1; i < churnHistoryRows.length; i++) {
      const storeId = churnHistoryRows[i][0];
      if (!churnHistoryByStore[storeId]) churnHistoryByStore[storeId] = [];
      churnHistoryByStore[storeId].push({
        churnMonth: churnHistoryRows[i][1] || '',
        firstChurnMonth: churnHistoryRows[i][1] || '',
        typeOfChurn: churnHistoryRows[i][3] || '',
        reason: churnHistoryRows[i][4] || ''
      });
    }

    const activeHistoryByStore = {};
    for (let i = 1; i < activeHistoryRows.length; i++) {
      const storeId = activeHistoryRows[i][0];
      if (!activeHistoryByStore[storeId]) activeHistoryByStore[storeId] = [];
      activeHistoryByStore[storeId].push({ activeMonth: activeHistoryRows[i][1] || '' });
    }
    
    const picCode = email.split('@')[0];
    const decenResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Decentralization!A:F'
    });
    const decenRows = decenResponse.data.values || [];
    const userDecen = decenRows.find(row => row[0] === picCode);
    const subteam = userDecen ? userDecen[1] : null;
    const role = userDecen ? userDecen[2] : null;
    const region = userDecen ? userDecen[3] : null;
    const team = userDecen ? userDecen[4] : null;
    const concat = userDecen ? userDecen[5] : null;

    const actionsByStore = {};
    const accessibleStoreIds = new Set();

    // Sử dụng dữ liệu BigQuery cho xác định quyền truy cập
    const storeRowsFromBigQuery = storeRows;
    
    if (role === 'Member') {
      storeRowsFromBigQuery.slice(1).forEach(row => {
        if (row[5] === picCode) accessibleStoreIds.add(row[0]);
      });
    } else if (role === 'Leader') {
      const subteamPICs = decenRows.filter(row => row[1] === subteam && row[0]).map(row => row[0]);
      storeRowsFromBigQuery.slice(1).forEach(row => {
        if (row[5] && subteamPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
      });
    } else if (role === 'Manager') {
      if (region === 'ALL') {
        storeRowsFromBigQuery.slice(1).forEach(row => accessibleStoreIds.add(row[0]));
      } else if (region === 'HCM') {
        const concatPICs = decenRows.filter(row => row[5] === concat).map(row => row[0]);
        storeRowsFromBigQuery.slice(1).forEach(row => {
          if (row[5] && concatPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
        });
      } else if (region === 'HN') {
        if (team === 'ALL') {
          const hnPICs = decenRows.filter(row => row[3] === 'HN').map(row => row[0]);
          storeRowsFromBigQuery.slice(1).forEach(row => {
            if (row[5] && hnPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
          });
        } else {
          const concatPICs = decenRows.filter(row => row[5] === concat).map(row => row[0]);
          storeRowsFromBigQuery.slice(1).forEach(row => {
            if (row[5] && concatPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
          });
        }
      }
    }

    // Xử lý dữ liệu churn database từ BigQuery
    for (let i = 1; i < churnDatabaseRows.length; i++) {
      const storeId = churnDatabaseRows[i][0];
      if (accessibleStoreIds.has(storeId)) {
        if (!actionsByStore[storeId]) actionsByStore[storeId] = [];
        actionsByStore[storeId].push({
          contactDate: churnDatabaseRows[i][2] || '',
          PIC: churnDatabaseRows[i][3] || '',
          subteam: churnDatabaseRows[i][4] || '',
          typeOfContact: churnDatabaseRows[i][5] || '',
          action: churnDatabaseRows[i][6] || '',
          note: churnDatabaseRows[i][7] || '',
          whyNotReawaken: churnDatabaseRows[i][8] || '',
          churnMonth: churnDatabaseRows[i][9] || '',
          linkHubspot: churnDatabaseRows[i][10] || ''
        });
      }
    }

    // Xử lý dữ liệu active database từ BigQuery
    for (let i = 1; i < activeDatabaseRows.length; i++) {
      const storeId = activeDatabaseRows[i][0];
      if (accessibleStoreIds.has(storeId)) {
        if (!actionsByStore[storeId]) actionsByStore[storeId] = [];
        actionsByStore[storeId].push({
          contactDate: activeDatabaseRows[i][2] || '',
          PIC: activeDatabaseRows[i][3] || '',
          subteam: activeDatabaseRows[i][4] || '',
          typeOfContact: activeDatabaseRows[i][5] || '',
          action: activeDatabaseRows[i][6] || '',
          note: activeDatabaseRows[i][7] || '',
          activeMonth: activeDatabaseRows[i][8] || '',
          whyNotReawaken: '',
          linkHubspot: activeDatabaseRows[i][9] || ''
        });
      }
    }

    const progressByStore = {};
    const allStoreIds = new Set([...Object.keys(churnHistoryByStore), ...Object.keys(activeHistoryByStore)]);

    allStoreIds.forEach(storeId => {
      if (!accessibleStoreIds.has(storeId)) return;

      if (!progressByStore[storeId]) progressByStore[storeId] = [];

      const churns = churnHistoryByStore[storeId] || [];
      churns.forEach((churn, index) => {
        const churnActions = (actionsByStore[storeId] || []).filter(action => action.churnMonth === churn.firstChurnMonth);
        churnActions.sort((a, b) => {
          const dateA = parseAndFormatDate(a.contactDate, 'YYYY-MM-DD');
          const dateB = parseAndFormatDate(b.contactDate, 'YYYY-MM-DD');
          return dateB - dateA;
        });

        progressByStore[storeId].push({
          churnMonth: churn.churnMonth,
          firstChurnMonth: churn.firstChurnMonth,
          typeOfChurn: churn.typeOfChurn,
          reason: churn.reason,
          actions: churnActions,
          churnIndex: index + 1
        });
      });

      const actives = activeHistoryByStore[storeId] || [];
      actives.forEach((active, index) => {
        const activeActions = (actionsByStore[storeId] || []).filter(action => action.activeMonth === active.activeMonth);
        if (activeActions.length > 0) {
          activeActions.sort((a, b) => {
            const dateA = parseAndFormatDate(a.contactDate, 'YYYY-MM-DD');
            const dateB = parseAndFormatDate(b.contactDate, 'YYYY-MM-DD');
            return dateB - dateA;
          });

          progressByStore[storeId].push({
            activeMonth: active.activeMonth,
            typeOfChurn: 'Active',
            reason: '',
            actions: activeActions,
            activeIndex: index + 1
          });
        }
      });

      progressByStore[storeId].sort((a, b) => {
        const monthA = a.firstChurnMonth || a.activeMonth || '';
        const monthB = b.firstChurnMonth || b.activeMonth || '';
        const [monthAVal, yearA] = monthA.split('/').map(Number);
        const [monthBVal, yearB] = monthB.split('/').map(Number);
        const dateA = new Date(yearA, monthAVal - 1);
        const dateB = new Date(yearB, monthBVal - 1);
        return dateB - dateA;
      });
    });

    if (progressByStore && Object.keys(progressByStore).length > 0) {
      cache.set(cacheKey, progressByStore);
    }
    res.json(progressByStore || {});
  } catch (error) {
    console.error('Error fetching progress:', error);
    res.status(500).json({ error: 'Failed to fetch progress', details: error.message });
  }
});

app.post('/active-history', async (req, res) => {
  const { storeId } = req.body;
  if (!storeId) return res.status(400).json({ error: 'Store ID is required' });

  try {
    const cacheKey = `active_history_${storeId}`;
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const bigquery = await getBigQueryClient();
    
    const query = `
      SELECT active_month
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Active_History\`
      WHERE store_id = @storeId
      ORDER BY active_month DESC
    `;
    
    const options = {
      query,
      params: {storeId}
    };

    const [rows] = await bigquery.query(options);
    
    const storeActiveHistory = rows.map(row => ({
      activeMonth: row.active_month || ''
    }));

    cache.set(cacheKey, storeActiveHistory);
    res.json(storeActiveHistory);
  } catch (error) {
    console.error('Error fetching active history:', error);
    
    // Thêm xử lý dự phòng nếu BigQuery không khả dụng
    try {
      const sheets = await getSheetsClient();
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Ex Active History!A:B'
      });
      const activeRows = response.data.values || [];
      const storeActiveHistory = activeRows
        .filter(row => row[0] === storeId)
        .map(row => ({ activeMonth: row[1] || '' }));
      
      cache.set(`active_history_${storeId}`, storeActiveHistory);
      res.json(storeActiveHistory);
      
    } catch (backupError) {
      console.error('Backup fetch also failed:', backupError);
      res.status(500).json({ error: 'Failed to fetch active history', details: error.message });
    }
  }
});

app.get('/dropdown-churn-actions', async (req, res) => {
  try {
    const sheets = await getSheetsClient();
    const cacheKey = 'dropdown_churn_actions';
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Dropdown Churn Action!A:B'
    });
    const rows = response.data.values || [];
    const churnActions = rows.slice(1).map(row => ({  // Sửa lỗi cú pháp ở đây
      typeOfChurn: row[0] || '',
      churnAction: row[1] || ''
    }));
    cache.set(cacheKey, churnActions);
    res.json(churnActions);
  } catch (error) {
    console.error('Error fetching dropdown churn actions:', error);
    res.status(500).json({ error: 'Failed to fetch dropdown churn actions', details: error.message });
  }
});

app.get('/dropdown-active-actions', async (req, res) => {
  try {
    const sheets = await getSheetsClient();
    const cacheKey = 'dropdown_active_actions';
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Dropdown Active Action!A:A'
    });
    const rows = response.data.values || [];
    const activeActions = rows.slice(1).map(row => row[0] || '');
    cache.set(cacheKey, activeActions);
    res.json(activeActions);
  } catch (error) {
    console.error('Error fetching dropdown active actions:', error);
    res.status(500).json({ error: 'Failed to fetch dropdown active actions', details: error.message });
  }
});

app.get('/dropdown-why-reasons', async (req, res) => {
  try {
    const sheets = await getSheetsClient();
    const cacheKey = 'dropdown_why_reasons';
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Dropdown Why!A:B'
    });
    const rows = response.data.values || [];
    const whyReasons = rows.slice(1).map (row => ({
      typeOfChurn: row[0] || '',
      whyNotReawaken: row[1] || ''
    }));
    cache.set(cacheKey, whyReasons);
    res.json(whyReasons);
  } catch (error) {
    console.error('Error fetching dropdown why reasons:', error);
    res.status(500).json({ error: 'Failed to fetch dropdown why reasons', details: error.message });
  }
});

// Keep-alive endpoint to prevent backend from sleeping
app.get('/keep-alive', (req, res) => {
  res.status(204).end(); // Trả về status 204 (No Content)
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// Hàm lấy dữ liệu Store Info từ BigQuery với xử lý lỗi
async function getStoreInfoFromBigQuery() {
  try {
    const bigquery = await getBigQueryClient();
    
    const query = `
      SELECT 
        store_id, 
        store_name, 
        buyer_id, 
        GROUP_STORE_NAME, 
        FINAL_CURRENT_TEAM, 
        FINAL_CURRENT_PIC, 
        STORE_CATEGORY, 
        STORE_SUB_CATEGORY, 
        PAYMENT_METHOD, 
        FULL_ADDRESS, 
        first_order_date,
        last_order_date, 
        Churn_this_month,
        No_Day_No_Buy
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Store_info\`
    `;
    
    const [rows] = await bigquery.query({query});
    
    // Format dữ liệu
    const header = [
      'store_id', 'store_name', 'buyer_id', 'buyer_name', 'register_date', 
      'current_pic', 'store_manager', 'account_type', 'channel', 'full_address', 
      'segmentation', 'last_order_date', 'churn_this_month'
    ];
    
    const formattedRows = rows.map(row => {
      // Chuyển đổi các ngày BigQueryDate sang định dạng chuỗi dd/mm/yyyy
      let lastOrderDateStr = '';
      let firstOrderDateStr = '';
      
      if (row.last_order_date && typeof row.last_order_date === 'object' && row.last_order_date.value) {
        const dateParts = row.last_order_date.value.split('-');
        if (dateParts.length === 3) {
          lastOrderDateStr = `${dateParts[2]}/${dateParts[1]}/${dateParts[0]}`;
        } else {
          lastOrderDateStr = row.last_order_date.value; // Fallback nếu không đúng định dạng
        }
      } else if (row.last_order_date) {
        lastOrderDateStr = String(row.last_order_date); // Đảm bảo chuyển sang string
      }
      
      if (row.first_order_date && row.first_order_date.value) {
        const dateParts = row.first_order_date.value.split('-');
        if (dateParts.length === 3) {
          firstOrderDateStr = `${dateParts[2]}/${dateParts[1]}/${dateParts[0]}`;
        }
      }
      
      return [
        row.store_id || '',
        row.store_name || '',
        row.buyer_id || '',
        row.GROUP_STORE_NAME || '',
        row.FINAL_CURRENT_TEAM || '',
        row.FINAL_CURRENT_PIC || '',
        row.STORE_CATEGORY || '',
        row.STORE_SUB_CATEGORY || '',
        row.PAYMENT_METHOD || '',
        row.FULL_ADDRESS || '',
        firstOrderDateStr,
        lastOrderDateStr,
        row.Churn_this_month || ''
      ];
    });
    
    return [header, ...formattedRows];
  } catch (error) {
    console.error('Error fetching data from BigQuery:', error);
    
    // Fallback to Google Sheets if BigQuery fails
    console.log('Falling back to Google Sheets...');
    const sheets = await getSheetsClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Store_info!A:M'
    });
    return response.data.values || [[]];
  }
}

// Hàm lấy dữ liệu Churn History từ BigQuery
async function getChurnHistoryFromBigQuery() {
  try {
    const bigquery = await getBigQueryClient();
    
    const query = `
      SELECT 
        StoreID as store_id,
        first_churn_month as churn_month,
        last_order_date as reporting_month,
        Type_of_churn as type_of_churn,
        Reason as reason
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Churn_History\`
    `;
    
    const [rows] = await bigquery.query({query});
    
    // Chuyển đổi để format giống Google Sheets
    const header = ['store_id', 'churn_month', 'reporting_month', 'type_of_churn', 'reason'];
    
    const formattedRows = rows.map(row => {
      // Xử lý đặc biệt cho first_churn_month (churn_month)
      let churnMonthStr = '';
      if (row.churn_month && typeof row.churn_month === 'object' && row.churn_month.value) {
        const dateParts = row.churn_month.value.split('-');
        if (dateParts.length === 3) {
          churnMonthStr = `${dateParts[2]}/${dateParts[1]}/${dateParts[0]}`;
        } else {
          churnMonthStr = String(row.churn_month.value);
        }
      } else {
        churnMonthStr = String(row.churn_month || '');
      }
      
      return [
        row.store_id || '',
        churnMonthStr,
        row.reporting_month || '',
        row.type_of_churn || '',
        row.reason || ''
      ];
    });
    
    return [header, ...formattedRows];
  } catch (error) {
    console.error('Error fetching Churn History from BigQuery:', error);
    
    // Fallback to Google Sheets
    console.log('Falling back to Google Sheets for Churn History...');
    const sheets = await getSheetsClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Churn History!A:E'
    });
    return response.data.values || [[]];
  }
}

// Hàm lấy dữ liệu Active History từ BigQuery
async function getActiveHistoryFromBigQuery() {
  try {
    const bigquery = await getBigQueryClient();
    
    const query = `
      SELECT *
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Active_History\`
    `;
    
    const [rows] = await bigquery.query({query});
    
    // Chuyển đổi để format giống Google Sheets
    const header = ['store_id', 'active_month'];
    
    const formattedRows = rows.map(row => {
      let activeMonthStr = '';
      
      if (row.active_month && typeof row.active_month === 'object' && row.active_month.value) {
        const dateParts = row.active_month.value.split('-');
        if (dateParts.length >= 2) {
          activeMonthStr = `${dateParts[1]}/${dateParts[0]}`; // mm/yyyy
        } else {
          activeMonthStr = String(row.active_month.value);
        }
      } else {
        activeMonthStr = String(row.active_month || '');
      }
      
      return [
        String(row.store_id || ''),
        activeMonthStr
      ];
    });
    
    return [header, ...formattedRows];
  } catch (error) {
    console.error('Error fetching Active History from BigQuery:', error);
    
    // Fallback to Google Sheets
    console.log('Falling back to Google Sheets for Active History...');
    const sheets = await getSheetsClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Active History!A:B'
    });
    return response.data.values || [[]];
  }
}

// Hàm đọc dữ liệu Churn Database từ BigQuery
async function getChurnDatabaseFromBigQuery() {
  try {
    const bigquery = await getBigQueryClient();
    
    const query = `
      SELECT 
        Store_ID,
        Store_Name,
        Contact_Date,
        PIC,
        Subteam,
        Type_of_Contact,
        Action,
        Note,
        Churn_Month,
        Link_Hubspot
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Churn_Database_table\`
      ORDER BY Contact_Date DESC
    `;
    
    const [rows] = await bigquery.query({query});
    
    // Chuyển đổi định dạng để tương thích với dữ liệu cũ
    const header = ['store_id', 'store_name', 'contact_date', 'pic', 'subteam', 'type_of_contact', 'action', 'note', 'churn_month', 'link_hubspot'];
    
    const formattedRows = rows.map(row => {
      // Xử lý đặc biệt cho contact_date nếu là đối tượng BigQueryDate
      let contactDateStr = formatBigQueryDate(row.Contact_Date);
      
      // Xử lý churn_month nếu là đối tượng BigQueryDate
      let churnMonthStr = formatBigQueryDate(row.Churn_Month);
      
      return [
        row.Store_ID || '',
        row.Store_Name || '',
        contactDateStr,
        row.PIC || '',
        row.Subteam || '',
        row.Type_of_Contact || '',
        row.Action || '',
        row.Note || '',
        row.Why_not_reawaken || '',
        churnMonthStr,
        row.Link_Hubspot || ''
      ];
    });
    
    return [header, ...formattedRows];
    
  } catch (error) {
    console.error('Error fetching Churn Database from BigQuery:', error);
    
    // Fallback to Google Sheets if BigQuery fails
    console.log('Falling back to Google Sheets for Churn Database...');
    const sheets = await getSheetsClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Churn Database!A:K'
    });
    return response.data.values || [[]];
  }
}

// Hàm đọc dữ liệu Active Database từ BigQuery
async function getActiveDatabaseFromBigQuery() {
  try {
    const bigquery = await getBigQueryClient();
    
    const query = `
      SELECT 
        Store_ID,
        Store_Name,
        Contact_Date,
        PIC,
        Subteam,
        Type_of_Contact,
        Action,
        Note,
        Active_Month,
        Link_Hubspot
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Active_Database_table\`
      ORDER BY Contact_Date DESC
    `;
    
    const [rows] = await bigquery.query({query});
    
    // Chuyển đổi định dạng để tương thích với dữ liệu cũ
    const header = ['store_id', 'store_name', 'contact_date', 'pic', 'subteam', 'type_of_contact', 'action', 'note', 'active_month', 'link_hubspot'];
    
    const formattedRows = rows.map(row => {
      // Xử lý đặc biệt cho contact_date
      let contactDateStr = formatBigQueryDate(row.Contact_Date);
      
      // Xử lý active_month đặc biệt (có thể ở định dạng mm/yyyy)
      let activeMonthStr = '';
      if (row.Active_Month && typeof row.Active_Month === 'object' && row.Active_Month.value) {
        // Nếu là định dạng yyyy-mm-dd, chuyển thành mm/yyyy
        const dateParts = row.Active_Month.value.split('-');
        if (dateParts.length >= 2) {
          activeMonthStr = `${dateParts[1]}/${dateParts[0]}`; // Chuyển từ yyyy-mm sang mm/yyyy
        } else {
          activeMonthStr = String(row.Active_Month.value);
        }
      } else {
        activeMonthStr = String(row.Active_Month || '');
      }
      
      return [
        row.Store_ID || '',
        row.Store_Name || '',
        contactDateStr,
        row.PIC || '',
        row.Subteam || '',
        row.Type_of_Contact || '',
        row.Action || '',
        row.Note || '',
        activeMonthStr,
        row.Link_Hubspot || ''
      ];
    });
    
    return [header, ...formattedRows];
    
  } catch (error) {
    console.error('Error fetching Active Database from BigQuery:', error);
    
    // Fallback to Google Sheets if BigQuery fails
    console.log('Falling back to Google Sheets for Active Database...');
    const sheets = await getSheetsClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Active Database!A:J'
    });
    return response.data.values || [[]];
  }
}

// Hàm tiện ích để định dạng ngày tháng từ BigQuery Date
function formatBigQueryDate(dateValue) {
  if (dateValue && typeof dateValue === 'object' && dateValue.value) {
    const dateParts = dateValue.value.split('-');
    if (dateParts.length === 3) {
      return `${dateParts[2]}/${dateParts[1]}/${dateParts[0]}`; // Chuyển từ yyyy-mm-dd sang dd/mm/yyyy
    } else {
      return String(dateValue.value);
    }
  } else {
    return String(dateValue || '');
  }
}

// Thêm endpoint mới vào file server.js

app.post('/export-data', async (req, res) => {
  const { email, storeIds, filters } = req.body;
  
  if (!email) {
    return res.status(400).json({ error: 'Email is required' });
  }
  
  try {
    // Lấy dữ liệu từ BigQuery cho Churn Database
    const bigquery = await getBigQueryClient();
    
    // Xây dựng điều kiện WHERE cho query
    let whereConditions = [];
    if (storeIds && storeIds.length > 0) {
      whereConditions.push(`Store_ID IN ('${storeIds.join("','")}')`);
    }
    
    if (filters.pic && filters.pic !== 'All') {
      whereConditions.push(`PIC = '${filters.pic}'`);
    }
    
    // Lấy Churn Database data
    const churnQuery = `
      SELECT 
        Store_ID,
        Store_Name,
        Contact_Date,
        PIC,
        Subteam,
        Type_of_Contact,
        Action,
        Note,
        Why_not_reawaken,
        Churn_Month,
        Link_Hubspot,
        'Churn' as Database_Type
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Churn_Database_table\`
      ${whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : ''}
      ORDER BY Contact_Date DESC
    `;
    
    // Lấy Active Database data
    const activeQuery = `
      SELECT 
        Store_ID,
        Store_Name,
        Contact_Date,
        PIC,
        Subteam,
        Type_of_Contact,
        Action,
        Note,
        Active_Month as Month,
        Link_Hubspot,
        'Active' as Database_Type
      FROM \`kamereo-351910.Sales_team_information_create_by_Phong_BOS.Active_Database_table\`
      ${whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : ''}
      ORDER BY Contact_Date DESC
    `;
    
    // Thực hiện cả hai query song song
    const [churnRows, activeRows] = await Promise.all([
      bigquery.query({query: churnQuery}).then(data => data[0]),
      bigquery.query({query: activeQuery}).then(data => data[0])
    ]);
    
    // Chuẩn hóa dữ liệu từ BigQuery
    const normalizeData = (rows) => {
      return rows.map(row => {
        // Xử lý các trường ngày tháng
        const processedRow = { ...row };
        
        // Xử lý Contact_Date
        if (row.Contact_Date && typeof row.Contact_Date === 'object' && row.Contact_Date.value) {
          const dateParts = row.Contact_Date.value.split('-');
          if (dateParts.length === 3) {
            processedRow.Contact_Date = `${dateParts[2]}/${dateParts[1]}/${dateParts[0]}`;
          }
        }
        
        // Xử lý Churn_Month
        if (row.Churn_Month && typeof row.Churn_Month === 'object' && row.Churn_Month.value) {
          const dateParts = row.Churn_Month.value.split('-');
          if (dateParts.length === 3) {
            processedRow.Churn_Month = `${dateParts[2]}/${dateParts[1]}/${dateParts[0]}`;
          }
        }
        
        // Xử lý Month (cho Active Database)
        if (row.Month && typeof row.Month === 'object' && row.Month.value) {
          const dateParts = row.Month.value.split('-');
          if (dateParts.length >= 2) {
            processedRow.Month = `${dateParts[1]}/${dateParts[0]}`;
          }
        }
        
        return processedRow;
      });
    };
    
    // Chuẩn hóa dữ liệu
    const normalizedChurnData = normalizeData(churnRows);
    const normalizedActiveData = normalizeData(activeRows);
    
    // Kết hợp cả hai nguồn dữ liệu
    const combinedData = [...normalizedChurnData, ...normalizedActiveData];
    
    // Lọc theo week nếu cần
    let filteredData = combinedData;
    if (filters.week && filters.week !== '') {
      try {      
        let weekStart, weekEnd;
        
        // Hỗ trợ nhiều định dạng week filter
        if (filters.week.includes('_to_')) {
          // Định dạng cũ: YYYY-MM-DD_to_YYYY-MM-DD
          const parts = filters.week.split('_to_');
          if (parts.length === 2) {
            const weekStartStr = parts[0];
            const weekEndStr = parts[1];
            
            // Hàm parse ngày an toàn - bảo vệ khỏi undefined input
            const parseExactDate = (dateStr) => {
              if (!dateStr) return null;
              const dateParts = dateStr.split('-');
              if (dateParts.length !== 3) return null;
              return new Date(parseInt(dateParts[0]), parseInt(dateParts[1]) - 1, parseInt(dateParts[2]));
            };
            
            weekStart = parseExactDate(weekStartStr);
            weekEnd = parseExactDate(weekEndStr);
          }
        } else {
          // Định dạng mới: một ngày đại diện cho tuần - các định dạng hỗ trợ
          const parseDate = (dateStr) => {
            if (!dateStr) return null;
            
            // Hỗ trợ nhiều định dạng ngày
            let year, month, day;
            
            if (dateStr.includes('-')) {
              // Định dạng YYYY-MM-DD hoặc DD-MM-YYYY
              const parts = dateStr.split('-');
              if (parts.length === 3) {
                if (parts[0].length === 4) {
                  // YYYY-MM-DD
                  [year, month, day] = parts.map(Number);
                } else {
                  // DD-MM-YYYY
                  [day, month, year] = parts.map(Number);
                }
                return new Date(year, month - 1, day);
              }
            } else if (dateStr.includes('/')) {
              // Định dạng DD/MM/YYYY
              const parts = dateStr.split('/');
              if (parts.length === 3) {
                [day, month, year] = parts.map(Number);
                return new Date(year, month - 1, day);
              }
            }
            return null;
          };
          
          // Ngày bắt đầu là ngày được chọn
          weekStart = parseDate(filters.week);
          
          // Ngày kết thúc là 6 ngày sau ngày bắt đầu
          if (weekStart) {
            weekEnd = new Date(weekStart);
            weekEnd.setDate(weekStart.getDate() + 6);
          }
        }
        
        // Chỉ tiếp tục nếu cả weekStart và weekEnd đều hợp lệ
        if (weekStart && weekEnd) {
          // Đặt thời gian cho weekStart là 00:00:00
          weekStart.setHours(0, 0, 0, 0);
          
          // Đặt thời gian cho weekEnd là 23:59:59
          weekEnd.setHours(23, 59, 59, 999);
                    
          // Lọc dữ liệu với xử lý nhiều định dạng ngày có thể có
          const beforeFilter = filteredData.length;
          filteredData = filteredData.filter(row => {
            if (!row.Contact_Date) return false;
            
            let contactDate;
            
            try {
              // Xử lý nhiều định dạng ngày có thể có
              if (typeof row.Contact_Date === 'string') {
                if (row.Contact_Date.includes('/')) {
                  // Định dạng DD/MM/YYYY
                  const [day, month, year] = row.Contact_Date.split('/').map(Number);
                  contactDate = new Date(year, month - 1, day);
                } else if (row.Contact_Date.includes('-')) {
                  // Định dạng YYYY-MM-DD hoặc DD-MM-YYYY
                  const parts = row.Contact_Date.split('-').map(Number);
                  if (parts[0] > 1000) {
                    // YYYY-MM-DD
                    contactDate = new Date(parts[0], parts[1] - 1, parts[2]);
                  } else {
                    // DD-MM-YYYY
                    contactDate = new Date(parts[2], parts[1] - 1, parts[0]);
                  }
                }
              } else if (row.Contact_Date instanceof Date) {
                contactDate = row.Contact_Date;
              }
              
              if (!contactDate || isNaN(contactDate)) return false;
              
              return contactDate >= weekStart && contactDate <= weekEnd;
            } catch (e) {
              console.log(`Error parsing date: ${row.Contact_Date}`, e);
              return false;
            }
          });
          
        } else {
          console.log('Invalid week format or date parsing failed:', filters.week);
        }
      } catch (error) {
        console.error('Error filtering by week:', error);
        // Vẫn tiếp tục thực thi với dữ liệu chưa được lọc
      }
    }
    
    // Lọc theo trạng thái nếu cần
    if (filters.status && filters.status !== 'All') {
      filteredData = filteredData.filter(row => 
        (filters.status === 'Churn' && row.Database_Type === 'Churn') ||
        (filters.status === 'Active' && row.Database_Type === 'Active')
      );
    }
    
    // Trả về dữ liệu đã lọc
    res.json({
      success: true,
      exportData: filteredData
    });
    
  } catch (error) {
    console.error('Error exporting data:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to export data', 
      details: error.message
    });
  }
});

// Update the motivation-messages endpoint

app.get('/motivation-messages', async (req, res) => {
  try {
    const force = req.query.force === 'true';
    const cacheKey = 'motivation_messages';
    
    // Skip cache if force=true
    if (!force && cache.get(cacheKey)) {
      return res.json(cache.get(cacheKey));
    }

    const sheets = await getSheetsClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Kamereo motivation!A:A'
    });

    const rows = response.data.values || [];
    const messages = rows.filter(row => row[0] && row[0].trim() !== '').map(row => row[0]);

    cache.set(cacheKey, messages, 3600); // Cache for 1 hour
    res.json(messages);
  } catch (error) {
    console.error('Error fetching motivation messages:', error);
    res.status(500).json({ error: 'Failed to fetch motivation messages' });
  }
});
const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
const NodeCache = require('node-cache');
const cache = new NodeCache({ stdTTL: 3600 }); // Cache 1 giờ

const app = express();
app.use(cors({ origin: '*' })); // Cho phép tất cả các nguồn
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

function parseDate(dateStr) {
  if (!dateStr) return new Date(0);
  const formats = [
    { pattern: /^(\d{2})\/(\d{2})\/(\d{4})$/, parse: ([_, d, m, y]) => new Date(`${y}-${m}-${d}`) },
    { pattern: /^(\d{4})-(\d{2})-(\d{2})$/, parse: ([_, y, m, d]) => new Date(`${y}-${m}-${d}`) }
  ];
  for (const { pattern, parse } of formats) {
    const match = dateStr.match(pattern);
    if (match) {
      const date = parse(match);
      if (!isNaN(date)) return date;
    }
  }
  console.error(`Cannot parse date: ${dateStr}`);
  return new Date(0);
}

function calculateDaysSinceLastOrder(lastOrderDate) {
  const lastOrder = parseDate(lastOrderDate);
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

  const storeResponse = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'Ex Store_info!A:M'
  });
  const storeRows = storeResponse.data.values || [];
  const store = storeRows.find(row => row[0] === storeId);
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
  return false;
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

    const storeResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Store_info!A:M'
    });
    const storeRows = storeResponse.data.values || [];
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
      const dateA = parseDate(a.lastOrderDate);
      const dateB = parseDate(b.lastOrderDate);
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

  let sheetName = ''; // Sửa: khai báo ngoài try
  try {
    const sheets = await getSheetsClient();

    // Kiểm tra quyền truy cập store
    const hasAccess = await checkStoreAccess(sheets, email, storeId);
    if (!hasAccess) {
      return res.status(403).json({ error: 'You do not have permission to record actions for this store' });
    }

    sheetName = type === 'Churn Database' ? 'Churn Database' : 'Active Database';
    const sheetRange = type === 'Churn Database' ? 'Churn Database!A:K' : 'Active Database!A:J';
    const values = type === 'Churn Database'
      ? [storeId, storeName || '', contactDate, PIC, subteam, typeOfContact, action, note || '', whyNotReawaken || '', churnMonthLastOrderDate, linkHubspot || '']
      : [storeId, storeName || '', contactDate, PIC, subteam, typeOfContact, action, note || '', activeMonth, linkHubspot || ''];

    const response = await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: sheetRange,
      valueInputOption: 'RAW',
      resource: { values: [values] }
    });

    if (!response.data.updates || response.data.updates.updatedRows !== 1) {
      console.error(`Failed to write data to ${sheetName}: No rows updated`);
      return res.status(500).json({ error: `Failed to write data to ${sheetName}: No rows updated` });
    }

    // Clear cache for related endpoints
    cache.del(`home_${email}`);
    cache.del(`progress_${email}`);
    res.json({ success: true });
  } catch (error) {
    console.error(`Error writing data to ${sheetName}:`, error); // sheetName luôn tồn tại
    res.status(error.code === 'ECONNRESET' ? 503 : 500).json({ error: `Failed to write data to ${sheetName}: ${error.message}` });
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
    const sheets = await getSheetsClient();

    const churnHistoryResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Churn History!A:E'
    });
    const churnHistoryRows = churnHistoryResponse.data.values || [];
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

    const activeHistoryResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Active History!A:B'
    });
    const activeHistoryRows = activeHistoryResponse.data.values || [];
    const activeHistoryByStore = {};
    for (let i = 1; i < activeHistoryRows.length; i++) {
      const storeId = activeHistoryRows[i][0];
      if (!activeHistoryByStore[storeId]) activeHistoryByStore[storeId] = [];
      activeHistoryByStore[storeId].push({ activeMonth: activeHistoryRows[i][1] || '' });
    }

    const churnDatabaseResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Churn Database!A:J'
    });
    const churnDatabaseRows = churnDatabaseResponse.data.values || [];

    const activeDatabaseResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Active Database!A:I'
    });
    const activeDatabaseRows = activeDatabaseResponse.data.values || [];

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

    const storeResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Store_info!A:M'
    });
    const storeRows = storeResponse.data.values || [];
    if (role === 'Member') {
      storeRows.slice(1).forEach(row => { // Bỏ qua dòng header
        if (row[5] === picCode) accessibleStoreIds.add(row[0]);
      });
    } else if (role === 'Leader') {
      const subteamPICs = decenRows.filter(row => row[1] === subteam && row[0]).map(row => row[0]);
      storeRows.slice(1).forEach(row => { // Bỏ qua dòng header
        if (row[5] && subteamPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
      });
    } else if (role === 'Manager') {
      if (region === 'ALL') {
        storeRows.slice(1).forEach(row => accessibleStoreIds.add(row[0])); // Bỏ qua dòng header
      } else if (region === 'HCM') {
        const concatPICs = decenRows.filter(row => row[5] === concat).map(row => row[0]);
        storeRows.slice(1).forEach(row => { // Bỏ qua dòng header
          if (row[5] && concatPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
        });
      } else if (region === 'HN') {
        if (team === 'ALL') {
          const hnPICs = decenRows.filter(row => row[3] === 'HN').map(row => row[0]);
          storeRows.slice(1).forEach(row => { // Bỏ qua dòng header
            if (row[5] && hnPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
          });
        } else {
          const concatPICs = decenRows.filter(row => row[5] === concat).map(row => row[0]);
          storeRows.slice(1).forEach(row => { // Bỏ qua dòng header
            if (row[5] && concatPICs.includes(row[5])) accessibleStoreIds.add(row[0]);
          });
        }
      }
    }

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
          churnMonth: churnDatabaseRows[i][9] || ''
        });
      }
    }

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
          whyNotReawaken: ''
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
          const dateA = parseDate(a.contactDate);
          const dateB = parseDate(b.contactDate);
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
            const dateA = parseDate(a.contactDate);
            const dateB = parseDate(b.contactDate);
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

    cache.set(cacheKey, progressByStore);
    res.json(progressByStore);
  } catch (error) {
    console.error('Error fetching progress:', error);
    res.status(500).json({ error: 'Failed to fetch progress', details: error.message });
  }
});

app.post('/active-history', async (req, res) => {
  const { storeId } = req.body;
  if (!storeId) return res.status(400).json({ error: 'Store ID is required' });

  try {
    const sheets = await getSheetsClient();
    const cacheKey = `active_history_${storeId}`;
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const activeHistoryResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ex Active History!A:B'
    });
    const activeHistoryRows = activeHistoryResponse.data.values || [];

    const storeActiveHistory = activeHistoryRows
      .filter(row => row[0] === storeId)
      .map(row => ({ activeMonth: row[1] || '' }))
      .sort((a, b) => {
        const [monthA, yearA] = a.activeMonth.split('/').map(Number);
        const [monthB, yearB] = b.activeMonth.split('/').map(Number);
        const dateA = new Date(yearA, monthA - 1);
        const dateB = new Date(yearB, monthB - 1);
        return dateB - dateA;
      });

    cache.set(cacheKey, storeActiveHistory);
    res.json(storeActiveHistory);
  } catch (error) {
    console.error('Error fetching active history:', error);
    res.status(500).json({ error: 'Failed to fetch active history', details: error.message });
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
    const churnActions = rows.slice(1).map(row => ({
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
    const whyReasons = rows.slice(1).map(row => ({
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
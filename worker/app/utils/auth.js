const AUTH_USERNAME = 'manishbsn188@gmail.com';
const AUTH_PASSWORD = 'preorder@beta';
const SESSION_COOKIE_NAME = 'rjs_preorder_session';
const SESSION_SECRET = 'rjs_preorder_secret_key_2024';

// Generate a simple session token
function generateSessionToken() {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2);
  return btoa(`${timestamp}:${random}:${SESSION_SECRET}`).replace(/[^a-zA-Z0-9]/g, '');
}

// Verify session token
function verifySessionToken(token) {
  try {
    const decoded = atob(token);
    const parts = decoded.split(':');
    if (parts.length === 3 && parts[2] === SESSION_SECRET) {
      const timestamp = parseInt(parts[0]);
      // Session valid for 24 hours
      const maxAge = 24 * 60 * 60 * 1000;
      if (Date.now() - timestamp < maxAge) {
        return true;
      }
    }
  } catch (e) {
    return false;
  }
  return false;
}

// Get session cookie from request
function getSessionCookie(request) {
  const cookieHeader = request.headers.get('Cookie');
  if (!cookieHeader) return null;
  
  const cookies = cookieHeader.split(';').map(c => c.trim());
  for (const cookie of cookies) {
    if (cookie.startsWith(`${SESSION_COOKIE_NAME}=`)) {
      return cookie.substring(SESSION_COOKIE_NAME.length + 1);
    }
  }
  return null;
}

// Check authentication (supports both Basic Auth and Session Cookie)
export function checkAuth(request) {
  // Check session cookie first
  const sessionToken = getSessionCookie(request);
  if (sessionToken && verifySessionToken(sessionToken)) {
    return true;
  }
  
  // Fall back to Basic Auth
  const authHeader = request.headers.get('Authorization');
  if (authHeader && authHeader.startsWith('Basic ')) {
    try {
      const base64 = authHeader.substring(6);
      const decoded = atob(base64);
      const [username, password] = decoded.split(':');
      
      if (username === AUTH_USERNAME && password === AUTH_PASSWORD) {
        return true;
      }
    } catch (e) {
      return false;
    }
  }
  
  return false;
}

// Require authentication - redirects to login if not authenticated
export function requireAuth(request) {
  if (!checkAuth(request)) {
    const url = new URL(request.url);
    const loginUrl = `/login.html?redirect=${encodeURIComponent(url.pathname + url.search)}`;
    
    return new Response(null, {
      status: 302,
      headers: {
        'Location': loginUrl,
        'Content-Type': 'text/html'
      }
    });
  }
  return null;
}

// Create session cookie
export function createSessionCookie() {
  const token = generateSessionToken();
  return `${SESSION_COOKIE_NAME}=${token}; Path=/; Max-Age=86400; HttpOnly; SameSite=Lax`;
}

// Verify login credentials
export function verifyCredentials(email, password) {
  return email === AUTH_USERNAME && password === AUTH_PASSWORD;
}


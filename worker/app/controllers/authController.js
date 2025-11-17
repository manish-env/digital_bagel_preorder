import { json } from '../utils/http.js';
import { verifyCredentials, createSessionCookie } from '../utils/auth.js';

export async function login(request, env) {
  try {
    const formData = await request.formData();
    const email = formData.get('email') || '';
    const password = formData.get('password') || '';
    
    if (verifyCredentials(email, password)) {
      const cookie = createSessionCookie();
      return json(
        { success: true, message: 'Login successful' },
        200,
        {
          'Set-Cookie': cookie
        }
      );
    } else {
      return json(
        { error: 'Invalid email or password' },
        401
      );
    }
  } catch (e) {
    return json(
      { error: 'Login failed', details: String(e?.message || e) },
      500
    );
  }
}

export async function logout(request, env) {
  const cookie = `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax`;
  return json(
    { success: true, message: 'Logged out successfully' },
    200,
    {
      'Set-Cookie': cookie
    }
  );
}










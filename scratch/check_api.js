
const axios = require('axios');

async function check() {
  try {
    const res = await axios.get('https://jacobs.strassburger.dev/api/jacobcontests');
    console.log(JSON.stringify(res.data.slice(0, 2), null, 2));
  } catch (e) {
    console.error(e.message);
  }
}

check();

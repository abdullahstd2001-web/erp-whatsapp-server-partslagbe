import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;

// POST /api/scrape-leads
// Expects: { query: string, location: string }
app.post('/api/scrape-leads', async (req, res) => {
  try {
    const { query, location } = req.body;
    
    if (!query || !location) {
      return res.status(400).json({ error: 'Both query and location are required.' });
    }

    const apiKey = process.env.GOOGLE_PLACES_API_KEY;

    // Simulated robust data generation if the API Key is not present or if simulation is requested
    if (!apiKey) {
      console.log('No GOOGLE_PLACES_API_KEY provided in .env. Returning simulated Google Places data.');
      
      // Simulate network delay
      await new Promise(resolve => setTimeout(resolve, 1500));

      const mockData = [
        {
          id: uuidv4(),
          googlePlaceId: `ChIJ_${uuidv4()}`,
          businessName: `${location} ${query} Hub`,
          address: `123 Main Road, ${location}`,
          phoneNumber: '+880 1711-000001',
          status: 'RAW_LEAD',
          rating: 4.5,
          createdAt: new Date().toISOString()
        },
        {
          id: uuidv4(),
          googlePlaceId: `ChIJ_${uuidv4()}`,
          businessName: `Star ${query} of ${location}`,
          address: `45 Commercial Area, ${location}`,
          phoneNumber: '+880 1811-000002',
          status: 'RAW_LEAD',
          rating: 4.8,
          createdAt: new Date().toISOString()
        },
        {
          id: uuidv4(),
          googlePlaceId: `ChIJ_${uuidv4()}`,
          businessName: `National ${query} Enterprise`,
          address: `88 Industrial Zone, ${location}`,
          phoneNumber: '+880 1911-000003',
          status: 'RAW_LEAD',
          rating: 4.2,
          createdAt: new Date().toISOString()
        },
        {
          id: uuidv4(),
          googlePlaceId: `ChIJ_${uuidv4()}`,
          businessName: `Mega ${query} Store`,
          address: `7B Market Plaza, ${location}`,
          phoneNumber: '+880 1611-000004',
          status: 'RAW_LEAD',
          rating: 3.9,
          createdAt: new Date().toISOString()
        },
        {
          id: uuidv4(),
          googlePlaceId: `ChIJ_${uuidv4()}`,
          businessName: `${location} Premium ${query}`,
          address: `100 VIP Road, ${location}`,
          phoneNumber: '+880 1511-000005',
          status: 'RAW_LEAD',
          rating: 4.7,
          createdAt: new Date().toISOString()
        }
      ];

      return res.json({ leads: mockData });
    }

    // Actual Google Places API Call
    // Utilizing Text Search API for flexible query + location (e.g. "Hardware Stores in Dhaka")
    const searchString = encodeURIComponent(`${query} in ${location}`);
    const googleApiUrl = `https://maps.googleapis.com/maps/api/place/textsearch/json?query=${searchString}&key=${apiKey}`;

    const response = await fetch(googleApiUrl);
    const data = await response.json();

    if (data.status !== 'OK') {
      console.error('Google API Error:', data.status, data.error_message);
      return res.status(500).json({ error: 'Failed to fetch from Google Places API', details: data.error_message || data.status });
    }

    const leads = data.results.map((place) => ({
      id: uuidv4(),
      googlePlaceId: place.place_id,
      businessName: place.name,
      address: place.formatted_address,
      phoneNumber: 'N/A', // Text Search doesn't return phone. Details API needed for phone. 
      status: 'RAW_LEAD',
      rating: place.rating || 0,
      createdAt: new Date().toISOString()
    }));

    return res.json({ leads });
  } catch (error) {
    console.error('Server error during scrape:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  console.log(`Lead Scraper Server running securely on port ${PORT}`);
});

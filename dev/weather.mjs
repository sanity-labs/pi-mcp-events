import {z} from 'zod';

export const subscriptionArguments = z.strictObject({stationId: z.string().min(1).optional()});
const condition = z.enum(['clear', 'cloudy', 'rain', 'storm']);
const reading = {
  stationId: z.string(), station: z.string(), condition,
  temperatureC: z.number(), windKph: z.number().nonnegative(),
  rainfallMmPerHour: z.number().nonnegative(), daylight: z.boolean(),
};
export const payloads = {
  'weather.reading': z.strictObject(reading),
  'weather.changed': z.strictObject({...reading, previousCondition: condition}),
  'weather.alert': z.strictObject({...reading, kind: z.literal('storm'), status: z.enum(['raised', 'cleared']), message: z.string()}),
};
export const catalog = Object.entries(payloads).map(([name, payload]) => ({
  name,
  description: {
    'weather.reading': 'Island weather: temperature (°C), wind (km/h), rain (mm/h), and daylight.',
    'weather.changed': 'Condition changes, staggered between stations over several minutes.',
    'weather.alert': 'A storm warning is raised or cleared once per transition, with the current reading.',
  }[name],
  delivery: ['poll', 'push'], inputSchema: z.toJSONSchema(subscriptionArguments), payloadSchema: z.toJSONSchema(payload),
}));

export function createWeather({now = Date.now} = {}) {
  const stations = [
    {stationId: 'harbor', station: 'Harbor', forecast: [
      {afterSeconds: 0, condition: 'cloudy', temperatureC: 22, windKph: 18, rainfallMmPerHour: 0},
      {afterSeconds: 5, condition: 'rain', temperatureC: 21, windKph: 26, rainfallMmPerHour: 3},
      {afterSeconds: 20, condition: 'storm', temperatureC: 20, windKph: 55, rainfallMmPerHour: 12},
      {afterSeconds: 110, condition: 'rain', temperatureC: 20, windKph: 28, rainfallMmPerHour: 4},
      {afterSeconds: 140, condition: 'cloudy', temperatureC: 21, windKph: 15, rainfallMmPerHour: 0},
      {afterSeconds: 170, condition: 'clear', temperatureC: 22, windKph: 9, rainfallMmPerHour: 0},
    ]},
    {stationId: 'hilltop', station: 'Hilltop', forecast: [
      {afterSeconds: 0, condition: 'clear', temperatureC: 19, windKph: 12, rainfallMmPerHour: 0},
      {afterSeconds: 35, condition: 'cloudy', temperatureC: 19, windKph: 20, rainfallMmPerHour: 0},
      {afterSeconds: 50, condition: 'rain', temperatureC: 18, windKph: 32, rainfallMmPerHour: 5},
      {afterSeconds: 65, condition: 'storm', temperatureC: 17, windKph: 65, rainfallMmPerHour: 16},
      {afterSeconds: 155, condition: 'rain', temperatureC: 17, windKph: 30, rainfallMmPerHour: 3},
      {afterSeconds: 170, condition: 'cloudy', temperatureC: 18, windKph: 18, rainfallMmPerHour: 0},
      {afterSeconds: 175, condition: 'clear', temperatureC: 19, windKph: 12, rainfallMmPerHour: 0},
    ]},
  ];
  const started = now();
  const previous = new Map();
  const warnings = new Map();
  let step = 0;
  const round = value => Math.round(value * 10) / 10;
  const read = station => {
    const time = now();
    const seconds = Math.max(0, (time - started) / 1000);
    const phase = seconds % 180;
    const conditions = station.forecast.findLast(item => item.afterSeconds <= phase);
    const variation = Math.sin(seconds / 30 + (station.stationId === 'harbor' ? 0 : 2));
    const hour = new Date(time).getUTCHours();
    return {
      stationId: station.stationId, station: station.station, condition: conditions.condition,
      temperatureC: round(conditions.temperatureC + variation * 0.3),
      windKph: Math.round(conditions.windKph + variation * 2),
      rainfallMmPerHour: conditions.rainfallMmPerHour === 0 ? 0 : round(conditions.rainfallMmPerHour + variation * 0.2),
      daylight: hour >= 6 && hour < 18,
    };
  };
  return {
    advance() {
      const data = read(stations[step++ % stations.length]);
      const before = previous.get(data.stationId);
      previous.set(data.stationId, data.condition);
      const events = [{name: 'weather.reading', data}];
      if (before && before !== data.condition) {
        events.push({name: 'weather.changed', data: {...data, previousCondition: before}});
      }
      return events;
    },
    alerts() {
      return stations.flatMap(station => {
        const data = read(station);
        const active = data.condition === 'storm';
        if (active === (warnings.get(data.stationId) ?? false)) return [];
        warnings.set(data.stationId, active);
        const status = active ? 'raised' : 'cleared';
        const message = active ? 'Storm warning: strong winds and heavy rain.' : 'Storm warning cleared. Rain is easing.';
        return [{name: 'weather.alert', data: {...data, kind: 'storm', status, message}}];
      });
    },
  };
}

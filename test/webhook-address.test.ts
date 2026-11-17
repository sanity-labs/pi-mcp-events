import {describe, expect, it} from 'vitest';
// @ts-expect-error The development server is plain JavaScript.
import {isPublicAddress} from '../dev/webhook-delivery.mjs';

const blockedIpv4 = [
  '0.0.0.0', '0.255.255.255', '10.0.0.0', '10.255.255.255', '100.64.0.0', '100.127.255.255',
  '127.0.0.0', '127.255.255.255', '169.254.0.0', '169.254.255.255', '172.16.0.0', '172.31.255.255',
  '192.0.0.0', '192.0.0.255', '192.0.2.0', '192.0.2.255', '192.31.196.0', '192.31.196.255',
  '192.52.193.0', '192.52.193.255', '192.88.99.0', '192.88.99.255', '192.168.0.0', '192.168.255.255',
  '192.175.48.0', '192.175.48.255', '198.18.0.0', '198.19.255.255', '198.51.100.0', '198.51.100.255',
  '203.0.113.0', '203.0.113.255', '224.0.0.0', '239.255.255.255', '240.0.0.0', '255.255.255.255',
];
const allowedIpv4 = [
  '1.0.0.0', '8.8.8.8', '9.255.255.255', '11.0.0.0', '100.63.255.255', '100.128.0.0',
  '126.255.255.255', '128.0.0.0', '169.253.255.255', '169.255.0.0', '172.15.255.255', '172.32.0.0',
  '192.0.1.0', '192.0.3.0', '192.31.195.255', '192.31.197.0', '192.52.192.255', '192.52.194.0',
  '192.88.98.255', '192.88.100.0', '192.167.255.255', '192.169.0.0', '192.175.47.255', '192.175.49.0',
  '198.17.255.255', '198.20.0.0', '198.51.99.255', '198.51.101.0', '203.0.112.255', '203.0.114.0', '223.255.255.255',
];
const blockedIpv6 = [
  '::', '::1', '::ffff:0:0:0', '::ffff:0:ffff:ffff', '64:ff9b::', '64:ff9b::ffff:ffff',
  '64:ff9b:1::', '64:ff9b:1:ffff:ffff:ffff:ffff:ffff', '100::', '100::ffff:ffff:ffff:ffff',
  '2001::', '2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff', '2001:1::1', '2001:3::1', '2001:4:112::1',
  '2001:db8::', '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff', '2002::', '2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
  '2620:4f:8000::', '2620:4f:8000:ffff:ffff:ffff:ffff:ffff', '3fff::', '3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff',
  '5f00::', '5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 'fc00::', 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
  'fe80::', 'febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 'fec0::', 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
  'ff00::', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
];
const allowedIpv6 = [
  '::2', '::7f00:1', '::fffe:ffff:ffff', '::ffff:1:0:0', '64:ff9a:ffff:ffff:ffff:ffff:ffff:ffff',
  '64:ff9b::1:0:0', '64:ff9b:0:ffff:ffff:ffff:ffff:ffff', '64:ff9b:2::', 'ff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
  '100:0:0:1::', '2001:200::', '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff', '2001:db9::', '2003::',
  '2620:4f:7fff:ffff:ffff:ffff:ffff:ffff', '2620:4f:8001::', '3ffe:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
  '3fff:1000::', '5eff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', '5f01::', 'fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
  'fe00::', 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff', '2001:4860:4860::8888', '2606:4700:4700::1111',
];

function ipv4Forms(address: string) {
  const octets = address.split('.').map(Number);
  const upper = ((octets[0]! << 8) | octets[1]!).toString(16);
  const lower = ((octets[2]! << 8) | octets[3]!).toString(16);
  return [address, `::ffff:${address}`, `::ffff:${upper}:${lower}`, `::${address}`, `::${address}%eth0`];
}

describe('weather webhook callback addresses', () => {
  it.each(blockedIpv4)('blocks %s and its mapped and legacy dotted forms', address => {
    for (const form of ipv4Forms(address)) expect(isPublicAddress(form), form).toBe(false);
  });

  it.each(allowedIpv4)('allows %s and its mapped and legacy dotted forms', address => {
    for (const form of ipv4Forms(address)) expect(isPublicAddress(form), form).toBe(true);
  });

  it.each(blockedIpv6)('blocks special-purpose IPv6 address %s', address => {
    expect(isPublicAddress(address)).toBe(false);
    expect(isPublicAddress(address.toUpperCase())).toBe(false);
    expect(isPublicAddress(`${address}%eth0`)).toBe(false);
  });

  it.each(allowedIpv6)('preserves the existing IPv6 allowance for %s', address => {
    expect(isPublicAddress(address)).toBe(true);
    expect(isPublicAddress(address.toUpperCase())).toBe(true);
    expect(isPublicAddress(`${address}%eth0`)).toBe(true);
  });

  it.each(['', 'localhost', '127.1', '2130706433', '0x7f000001', '0177.0.0.1', '127.000.0.1',
    '[::1]', '127.0.0.1:443', '1.2.3.999', 'https://8.8.8.8', '::ffff:127.000.0.1'])('rejects invalid address %j', address => {
    expect(isPublicAddress(address)).toBe(false);
  });
});

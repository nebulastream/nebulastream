import { test, expect, type Page } from '@playwright/test';

async function addWorker(page: Page) {
  await page.getByTestId('add-worker-btn').click();
}

async function addSourceToWorker(page: Page) {
  await page.locator('.worker-node').click();
  await page.waitForTimeout(100);
  await page.getByTestId('add-source-btn').click();
  await page.waitForTimeout(100);
}

async function addSinkToWorker(page: Page) {
  await page.locator('.worker-node').click();
  await page.waitForTimeout(100);
  await page.getByTestId('add-sink-btn').click();
  await page.waitForTimeout(100);
}

test.describe('State persistence and reset', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForSelector('.react-flow');
  });

  test('topology survives page reload', async ({ page }) => {
    // Add a worker with a source and sink
    await addWorker(page);
    await addSourceToWorker(page);
    await addSinkToWorker(page);

    await expect(page.locator('.worker-node')).toHaveCount(1);
    await expect(page.locator('.source-node')).toHaveCount(1);
    await expect(page.locator('.sink-node')).toHaveCount(1);

    // Wait for persist middleware to flush to localStorage
    await page.waitForTimeout(200);

    // Reload the page
    await page.reload();
    await page.waitForSelector('.react-flow');

    // All nodes should still be there
    await expect(page.locator('.worker-node')).toHaveCount(1);
    await expect(page.locator('.source-node')).toHaveCount(1);
    await expect(page.locator('.sink-node')).toHaveCount(1);
    // Edges too (source→worker, sink→worker)
    await expect(page.locator('.react-flow__edge')).toHaveCount(2);
  });

  test('multiple workers persist across reload', async ({ page }) => {
    await addWorker(page);
    await addWorker(page);
    await expect(page.locator('.worker-node')).toHaveCount(2);

    await page.waitForTimeout(200);
    await page.reload();
    await page.waitForSelector('.react-flow');

    await expect(page.locator('.worker-node')).toHaveCount(2);
  });

  test('reset button clears all nodes', async ({ page }) => {
    await addWorker(page);
    await addSourceToWorker(page);
    await addSinkToWorker(page);

    await expect(page.locator('.react-flow__node')).toHaveCount(3);

    // Click reset
    await page.getByTestId('reset-btn').click();
    await page.waitForTimeout(200);

    await expect(page.locator('.react-flow__node')).toHaveCount(0);
    await expect(page.locator('.react-flow__edge')).toHaveCount(0);
  });

  test('reset persists across reload (empty state saved)', async ({ page }) => {
    await addWorker(page);
    await expect(page.locator('.worker-node')).toHaveCount(1);

    await page.waitForTimeout(200);

    // Reset
    await page.getByTestId('reset-btn').click();
    await page.waitForTimeout(200);
    await expect(page.locator('.react-flow__node')).toHaveCount(0);

    // Reload — should still be empty
    await page.reload();
    await page.waitForSelector('.react-flow');
    await expect(page.locator('.react-flow__node')).toHaveCount(0);
  });

  test('reset button is visible in the toolbar', async ({ page }) => {
    await expect(page.getByTestId('reset-btn')).toBeVisible();
    await expect(page.getByTestId('reset-btn')).toContainText('Reset');
  });
});

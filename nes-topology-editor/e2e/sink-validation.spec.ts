import { test, expect, type Page } from '@playwright/test';

async function waitForValidator(page: Page) {
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'loading', { timeout: 15_000 });
  await page.waitForTimeout(500);
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'validating', { timeout: 5_000 });
}

async function waitForValidation(page: Page) {
  await page.waitForTimeout(500);
  await expect(page.getByTestId('status-bar')).not.toHaveAttribute('data-status', 'validating', { timeout: 5_000 });
}

test.describe('Sink validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.waitForSelector('.react-flow');
    await waitForValidator(page);
  });

  test('Print sink with name validates successfully', async ({ page }) => {
    // Add worker
    await page.getByTestId('add-worker-btn').click();
    await page.waitForTimeout(200);

    // Select worker and add sink
    await page.locator('.worker-node').click();
    await page.waitForTimeout(100);
    await page.getByTestId('add-sink-btn').click();
    await page.waitForTimeout(200);

    // Sink should be selected — set the name
    const nameInput = page.locator('input[type="text"]').first();
    await nameInput.fill('MY_SINK');
    await page.waitForTimeout(100);

    // Wait for validation
    await waitForValidation(page);

    // Open YAML overlay to check what's being sent
    await page.getByTestId('yaml-toggle-btn').click();
    await page.waitForTimeout(300);

    // Take screenshot for debugging
    await page.screenshot({ path: 'test-results/sink-validation.png', fullPage: true });

    // Check the status bar — should be valid (no input_format error)
    const statusBar = page.getByTestId('status-bar');
    const status = await statusBar.getAttribute('data-status');
    const statusText = await statusBar.textContent();

    // If invalid, log the error for debugging
    if (status !== 'valid') {
      console.log('Validation status:', status);
      console.log('Status text:', statusText);
    }

    expect(status).toBe('valid');
  });
});
